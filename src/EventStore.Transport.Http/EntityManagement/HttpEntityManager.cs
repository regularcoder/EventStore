using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Principal;
using System.Text;
using System.Threading;
using EventStore.Common.Log;
using EventStore.Common.Utils;

namespace EventStore.Transport.Http.EntityManagement
{
    public sealed class HttpEntityManager
    {
        private static readonly ILogger Log = LogManager.GetLoggerFor<HttpEntityManager>();

        public object AsyncState { get; set; }
        public readonly HttpEntity HttpEntity;

        public bool IsProcessing
        {
            get { return _processing != 0; }
        }

        private int _processing;
        private readonly string[] _allowedMethods;
        private readonly Action<HttpEntity> _onRequestSatisfied;
        private Stream _currentOutputStream;
        private AsyncQueuedBufferWriter _asyncWriter;
        private readonly ICodec _requestCodec;
        private readonly ICodec _responseCodec;
        private readonly Uri _requestedUrl;
        private readonly bool _logHttpRequests;
        public readonly DateTime TimeStamp;

        internal HttpEntityManager(
            HttpEntity httpEntity, string[] allowedMethods, Action<HttpEntity> onRequestSatisfied, ICodec requestCodec,
            ICodec responseCodec, bool logHttpRequests)
        {
            Ensure.NotNull(httpEntity, "httpEntity");
            Ensure.NotNull(allowedMethods, "allowedMethods");
            Ensure.NotNull(onRequestSatisfied, "onRequestSatisfied");

            HttpEntity = httpEntity;
            TimeStamp = DateTime.UtcNow;

            _allowedMethods = allowedMethods;
            _onRequestSatisfied = onRequestSatisfied;
            _requestCodec = requestCodec;
            _responseCodec = responseCodec;
            _requestedUrl = httpEntity.RequestedUrl;
            _logHttpRequests = logHttpRequests;

            if (HttpEntity.Request != null && HttpEntity.Request.ContentLength64 == 0)
            {
                LogRequest(new byte[0]);
            }
        }

        public ICodec RequestCodec { get { return _requestCodec; } }
        public ICodec ResponseCodec { get { return _responseCodec; } }
        public Uri RequestedUrl { get { return _requestedUrl; } }
        public IPrincipal User { get { return HttpEntity.User; } }

        private void SetResponseCode(int code)
        {
            try
            {
                HttpEntity.Response.StatusCode = code;
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
            catch (ProtocolViolationException e)
            {
                Log.ErrorException(e, "Attempt to set invalid HTTP status code occurred.");
            }
        }

        private void SetResponseDescription(string desc)
        {
            try
            {
                HttpEntity.Response.StatusDescription = desc;
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
            catch (ArgumentException e)
            {
                Log.ErrorException(e, "Description string '{0}' did not pass validation. Status description was not set.", desc);
            }
        }

        private void SetContentType(string contentType, Encoding encoding)
        {
            try
            {
                HttpEntity.Response.ContentType = contentType + (encoding != null ? ("; charset=" + encoding.WebName) : "");
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
            catch (InvalidOperationException e)
            {
                Log.Debug("Error during setting content type on HTTP response: {0}.", e.Message);
            }
            catch (ArgumentOutOfRangeException e)
            {
                Log.ErrorException(e, "Invalid response type.");
            }
        }

        private void SetResponseLength(long length)
        {
            try
            {
                HttpEntity.Response.ContentLength64 = length;
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
            catch (InvalidOperationException e)
            {
                Log.Debug("Error during setting content length on HTTP response: {0}.", e.Message);
            }
            catch (ArgumentOutOfRangeException e)
            {
                Log.ErrorException(e, "Attempt to set invalid value '{0}' as content length.", length);
            }
        }

        private void SetRequiredHeaders()
        {
            try
            {
                HttpEntity.Response.AddHeader("Access-Control-Allow-Methods", string.Join(", ", _allowedMethods));
                HttpEntity.Response.AddHeader("Access-Control-Allow-Headers", "Content-Type, X-Requested-With, X-Forwarded-Host, X-Forwarded-Prefix, X-PINGOTHER, Authorization, ES-LongPoll, ES-ExpectedVersion, ES-EventId, ES-EventType, ES-RequiresMaster, ES-HardDelete, ES-ResolveLinkTos");
                HttpEntity.Response.AddHeader("Access-Control-Allow-Origin", "*");
                HttpEntity.Response.AddHeader("Access-Control-Expose-Headers", "Location, ES-Position, ES-CurrentVersion");
                if (HttpEntity.Response.StatusCode == HttpStatusCode.Unauthorized)
                    HttpEntity.Response.AddHeader("WWW-Authenticate", "Basic realm=\"ES\"");
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
            catch (Exception e)
            {
                Log.Debug("Failed to set required response headers: {0}.", e.Message);
            }
        }

        private void SetAdditionalHeaders(IEnumerable<KeyValuePair<string, string>> headers)
        {
            try
            {
                foreach (var kvp in headers)
                {
                    HttpEntity.Response.AddHeader(kvp.Key, kvp.Value);
                }
            }
            catch (ObjectDisposedException)
            {
                // ignore
            }
            catch (Exception e)
            {
                Log.Debug("Failed to set additional response headers: {0}.", e.Message);
            }
        }

        public void ReadRequestAsync(Action<HttpEntityManager, byte[]> onReadSuccess, Action<Exception> onError)
        {
            Ensure.NotNull(onReadSuccess, "OnReadSuccess");
            Ensure.NotNull(onError, "onError");

            var state = new ManagerOperationState(
                HttpEntity.Request.InputStream, new MemoryStream(), onReadSuccess, onError);
            var copier = new AsyncStreamCopier<ManagerOperationState>(
                state.InputStream, state.OutputStream, state, RequestRead);
            copier.Start();
        }

        public bool BeginReply(
            int code, string description, string contentType, Encoding encoding,
            IEnumerable<KeyValuePair<string, string>> headers)
        {
            if (HttpEntity.Response == null) // test instance
                return false;

            bool isAlreadyProcessing = Interlocked.CompareExchange(ref _processing, 1, 0) == 1;
            if (isAlreadyProcessing)
                return false;

            SetResponseCode(code);
            SetResponseDescription(description);
            SetContentType(contentType, encoding);
            SetRequiredHeaders();
            SetAdditionalHeaders(headers.Safe());
            return true;
        }

        public void ContinueReply(byte[] response, Action<Exception> onError, Action onCompleted)
        {
            Ensure.NotNull(onError, "onError");
            Ensure.NotNull(onCompleted, "onCompleted");

            _currentOutputStream = HttpEntity.Response.OutputStream;
            ContinueWriteResponseAsync(response, () => { }, onError, onCompleted);
        }

        private void DisposeStreamAndCloseConnection(string message)
        {
            IOStreams.SafelyDispose(_currentOutputStream);
            _currentOutputStream = null;
            CloseConnection(e => Log.Debug(message + "\nException: " + e.Message));
        }

        public void EndReply()
        {
            EndWriteResponse();
        }

        public void Reply(
            byte[] response, int code, string description, string contentType, Encoding encoding,
            IEnumerable<KeyValuePair<string, string>> headers, Action<Exception> onError)
        {
            Ensure.NotNull(onError, "onError");

            if (!BeginReply(code, description, contentType, encoding, headers))
                return;

            if (response == null || response.Length == 0)
            {
                LogResponse(new byte[0]);
                SetResponseLength(0);
                CloseConnection(onError);
            }
            else
            {
                LogResponse(response);
                SetResponseLength(response.Length);
                BeginWriteResponse();
                ContinueWriteResponseAsync(response, () => { }, onError, () => { });
                EndWriteResponse();
            }
        }

        public void ForwardReply(HttpResponseMessage response, Action<Exception> onError)
        {
            Ensure.NotNull(response, "response");
            Ensure.NotNull(onError, "onError");

            if (Interlocked.CompareExchange(ref _processing, 1, 0) != 0)
                return;
            
            try
            {
                HttpEntity.Response.StatusCode = (int)response.StatusCode;
                HttpEntity.Response.StatusDescription = response.ReasonPhrase;
                if(response.Content != null) 
                {
                    if(response.Content.Headers.ContentType != null) {
                        HttpEntity.Response.ContentType = response.Content.Headers.ContentType.MediaType;
                    }
                    HttpEntity.Response.ContentLength64 = response.Content.Headers.ContentLength.GetValueOrDefault();
                }
                foreach (var header in response.Headers)
                {
                    string headerValue;
                    switch (header.Key)
                    {
                        case "Content-Length": break;
                        case "Keep-Alive": break;
                        case "Transfer-Encoding": break;
                        case "WWW-Authenticate": 
                            headerValue = header.Value.FirstOrDefault();
                            HttpEntity.Response.AddHeader(header.Key, headerValue); 
                            break;

                        default:
                            headerValue = header.Value.FirstOrDefault();
                            HttpEntity.Response.Headers.Add(header.Key, headerValue);
                            break;
                    }
                }

                if (HttpEntity.Response.ContentLength64 > 0)
                {
                    response.Content.ReadAsStreamAsync()
                        .ContinueWith(task => {
                            new AsyncStreamCopier<HttpListenerResponse>(
                                task.Result, 
                                HttpEntity.Response.OutputStream, 
                                HttpEntity.Response,
                                copier => {
                                    Helper.EatException(HttpEntity.Response.Close);
                                }).Start();
                        });
                }
                else
                {
                    Helper.EatException(HttpEntity.Response.Close);
                }
            }
            catch (Exception e)
            {
                Log.ErrorException(e, "Failed to set up forwarded response parameters for '{0}'.", RequestedUrl);
            }
        }

        private void EndWriteResponse()
        {
            _asyncWriter.AppendDispose(exception => { });
        }

        private void BeginWriteResponse()
        {
            _currentOutputStream = HttpEntity.Response.OutputStream;
        }

        private void ContinueWriteResponseAsync(
            byte[] response, Action onSuccess, Action<Exception> onError, Action onCompleted)
        {
            if (_asyncWriter == null)
                _asyncWriter = new AsyncQueuedBufferWriter(
                    _currentOutputStream, () => DisposeStreamAndCloseConnection("Close connection error"));

            _asyncWriter.Append(
                response, errorIfAny =>
                    {
                        if (errorIfAny == null)
                            onSuccess();
                        else
                            onError(errorIfAny);
                        onCompleted();
                    });
        }

        private void RequestRead(AsyncStreamCopier<ManagerOperationState> copier)
        {
            var state = copier.AsyncState;

            if (copier.Error != null)
            {
                state.Dispose();
                CloseConnection(exc => Log.Debug("Close connection error (after crash in read request): {0}", exc.Message));

                state.OnError(copier.Error);
                return;
            }

            state.OutputStream.Seek(0, SeekOrigin.Begin);
            var memory = (MemoryStream) state.OutputStream;

            var request = memory.GetBuffer();
            if (memory.Length != memory.GetBuffer().Length)
            {
                request = new byte[memory.Length];
                Buffer.BlockCopy(memory.GetBuffer(), 0, request, 0, (int) memory.Length);
            }
            LogRequest(request);
            state.OnReadSuccess(this, request);
        }

        private void CloseConnection(Action<Exception> onError)
        {
            try
            {
                _onRequestSatisfied(HttpEntity);
                HttpEntity.Response.Close();
            }
            catch (Exception e)
            {
                onError(e);
            }
        }

        private string CreateHeaderLog(NameValueCollection headers)
        {
            var logBuilder = new StringBuilder();
            foreach (var header in HttpEntity.Request.Headers)
            {
                logBuilder.AppendFormat("{0}: {1}\n", header.ToString(), HttpEntity.Request.Headers[header.ToString()]);
            }
            return logBuilder.ToString();
        }

        private void LogRequest(byte[] body)
        {
            if (_logHttpRequests)
            {
                var logBuilder = new StringBuilder();
                logBuilder.AppendLine("HTTP Request Received");
                logBuilder.AppendFormat("{0}\n", DateTime.Now);
                logBuilder.AppendFormat("From: {0}\n", HttpEntity.Request.RemoteEndPoint.ToString());
                logBuilder.AppendFormat("{0} {1}\n", HttpEntity.Request.HttpMethod, HttpEntity.Request.Url);
                logBuilder.AppendLine(CreateHeaderLog(HttpEntity.Request.Headers));
                if (body != null && body.Length > 0)
                {
                    logBuilder.AppendLine(System.Text.Encoding.Default.GetString(body));
                }
                Log.Debug(logBuilder.ToString());
            }
        }

        private void LogResponse(byte[] body)
        {
            if (_logHttpRequests)
            {
                var logBuilder = new StringBuilder();
                logBuilder.AppendLine("HTTP Response");
                logBuilder.AppendFormat("{0}\n", DateTime.Now);
                logBuilder.AppendFormat("{0} {1}\n", HttpEntity.Response.StatusCode, HttpEntity.Response.StatusDescription);
                logBuilder.AppendLine(CreateHeaderLog(HttpEntity.Response.Headers));
                if (body != null && body.Length > 0)
                {
                    logBuilder.AppendLine(System.Text.Encoding.Default.GetString(body));
                }
                Log.Debug(logBuilder.ToString());
            }
        }
    }
}
