#lang racket/base

(require racket/exn
         racket/format
         racket/generic
         racket/match
         racket/port
         syntax/parse/define
         net/http-easy
         net/url
         json)

(define runtime-api-version "2018-06-01")

(struct exn:fail:aws-lambda:config exn:fail ())

(struct aws-lambda-service [next-handler response-handler error-handler])

(struct aws-lambda-config [service api-url task-root handler-name])

(struct aws-lambda-event-context [config headers request-id body])

(define (->service v)
  (cond
    [(aws-lambda-config? v)
     (aws-lambda-config-service v)]
    [(aws-lambda-event-context? v)
     (->service (aws-lambda-event-context-config v))]
    [else
      (error '->service "cannot find service table for ~a" v)]))

(define-syntax-rule (define-service-handler name ref c args ...)
  (define (name c args ...)
    (define handler (ref (->service c)))
    (handler c args ...)))

(define-service-handler
  aws-lambda-next-event
  aws-lambda-service-next-handler
  cfg)

(define-service-handler
  aws-lambda-event-response
  aws-lambda-service-response-handler
  ctx val)

(define-service-handler
  aws-lambda-event-error
  aws-lambda-service-error-handler
  ctx err)

(define-generics aws-lambda-serialize
  [aws-lambda-serialize-result aws-lambda-serialize]
  #:fast-defaults
  ([void?
    (define (aws-lambda-serialize-result v) #"")]
   [string?
    (define (aws-lambda-serialize-result v) v)]
   [bytes?
    (define (aws-lambda-serialize-result v) v)])
  #:defaults
  ([jsexpr?
    (define (aws-lambda-serialize-result v)
      (jsexpr->bytes v))]))

(define-generics aws-lambda-serialize-error
  [aws-lambda-serialize-error aws-lambda-serialize-error]
  [aws-lambda-error-type      aws-lambda-serialize-error]
  [aws-lambda-error-message   aws-lambda-serialize-error]
  #:defaults
  ([exn?])
  #:fallbacks
  [(define/generic etype aws-lambda-error-type)
   (define/generic emsg  aws-lambda-error-message)
   (define (aws-lambda-error-type e)    "Unknown")
   (define (aws-lambda-error-message e) (exn->string e))
   (define (aws-lambda-serialize-error e)
     (hash 'errorType    (etype e)
           'errorMessage (emsg e)))])

(define (make-next-event-url cfg)
  (combine-url/relative
    (aws-lambda-config-api-url cfg) "/runtime/invocation/next"))

(define (make-event-url ctx kind)
  (define reqid (aws-lambda-event-context-request-id ctx))
  (combine-url/relative
    (aws-lambda-config-api-url (aws-lambda-event-context-config ctx))
    (format "runtime/invocation/~a/~a" reqid kind)))

(define (make-event-response-url ctx)
  (make-event-url ctx 'response))

(define (make-event-error-url ctx)
  (make-event-url ctx 'error))

(define (check-200-status rsp)
  (unless (= 200 (response-status-code rsp))
    (error 'check-200-status
           "response not status 200: ~a" rsp)))

(define (aws-lambda-next-event/impl cfg)
  (define rsp (get (make-next-event-url cfg)))
  (check-200-status rsp)
  (define hdrs (response-headers rsp))
  (define req-id (response-headers-ref rsp 'lambda-runtime-aws-request-id))
  (define body (response-json rsp))
  (define ctx (aws-lambda-event-context cfg hdrs req-id body))
  (response-close! rsp)
  ctx)

(define (aws-lambda-event-response/impl ctx result)
  (post (make-event-response-url ctx)
        #:data (aws-lambda-serialize-result result)))

(define (aws-lambda-event-error/impl ctx err)
  (post (make-event-error-url ctx)
        #:json (aws-lambda-serialize-error err)))

(define default-aws-lambda-service
  (aws-lambda-service aws-lambda-next-event/impl
                      aws-lambda-event-response/impl
                      aws-lambda-event-error/impl))

(define (split-handler-name name)
  (match name
    [(pregexp #px"^([^:]+):(.*)$" (list _ modname funcname))
     (values (list 'lib modname) (string->symbol funcname))]))

(define (aws-lambda-initialize-handler cfg)
  (define task-root-path
    (path->complete-path (aws-lambda-config-task-root cfg)))
  (current-library-collection-paths
    (cons task-root-path (current-library-collection-paths)))
  (define-values (modname funcname)
    (split-handler-name (aws-lambda-config-handler-name cfg)))
  (dynamic-require modname funcname))

(define (aws-lambda-run1 cfg handler)
  (define ctx (aws-lambda-next-event cfg))
  (match (with-handlers ([exn:fail? values])
           (handler ctx))
    [(? exn? e) (aws-lambda-event-error ctx e)]
    [result     (aws-lambda-event-response ctx result)]))

(define (aws-lambda-run cfg handler)
  (aws-lambda-run1 cfg handler)
  (aws-lambda-run cfg handler))

(define (make-aws-lambda-config-from-environment)
  (define-simple-macro (getenv-or-fail varname:str)
    (or (getenv varname)
        (raise (exn:fail:aws-lambda:config (~a varname " not set")))))
  (define api-url
    (let ([u (getenv-or-fail "AWS_LAMBDA_RUNTIME_API")])
      (combine-url/relative (string->url u) runtime-api-version)))
  (define handler (getenv-or-fail "_HANDLER"))
  (define task-root (getenv-or-fail "LAMBDA_TASK_ROOT"))
  (aws-lambda-config default-aws-lambda-service api-url task-root handler))

(define (make-aws-lambda-test-config task-root handler-name)
  (define (read-event cfg)
    (define o (read-json))
    (define hdrs
      (for/list ([hdr-line (hash-ref o 'headers)])
        (string->bytes/utf-8 hdr-line)))
    (define body (hash-ref o 'body))
    (aws-lambda-event-context cfg hdrs #f body))

  (define (write-event-response rsp)
    (define proc
      (match (aws-lambda-serialize-result rsp)
        [(and (or (? bytes?) (? string?) (? input-port?)) val)
         (pure-payload val)]
        [proc proc]))
    (define-values (hdrs data) (proc (hash)))
    (for ([(k v) (in-hash hdrs)])
      (displayln (~a k ": " v)))
    (newline)
    (cond
      [(string? data)
       (write-string data)]
      [(bytes? data)
       (write-bytes data)]
      [(input-port? data)
       (copy-port data (current-output-port))])
    (newline))

  (define service
    (aws-lambda-service
      read-event
      (lambda (ctx val)
        (displayln 'OK)
        (write-event-response val))
      (lambda (ctx err)
        (displayln 'ERROR)
        (write-event-response
          (aws-lambda-serialize-error err)))))
  (aws-lambda-config service #f task-root handler-name))

(module* main #f
  (define cfg (make-aws-lambda-test-config "test-root" "bar:hello-world"))
  (define handler (aws-lambda-initialize-handler cfg))
  (aws-lambda-run1 cfg handler))

