#lang racket/base

(require racket/format
         racket/match
         racket/string
         net/http-client
         json)

(define fname (getenv "AWS_LAMBDA_FUNCTION_NAME"))
(define fver (getenv "AWS_LAMBDA_FUNCTION_VERSION"))

(define api (getenv "AWS_LAMBDA_RUNTIME_API"))
(define runtime-api-version "2018-06-01")
(define handler-location (getenv "_HANDLER"))

(define-values (handler-file handler-name)
  (match (string-split handler-location ":")
    [(list modname funcname)
     (values (string-append modname ".rkt")
             funcname)]))

(define-values (endpoint port)
  (match (string-split api ":")
    [(list host port)
     (values host (string->number port))]))

(define (gen-event-context headers)
  (let ([hashed (make-hash
                 (map
                  (lambda (v)
                    (let* ([vs (string-split (bytes->string/utf-8 v) ":")]
                           [key (string-trim (car vs))]
                           [val (string-trim (string-join (cdr vs) ":"))])
                      (cons key val)))
                  headers))])
    (hash
     'fn-name fname
     'fn-ver fver
     'fn-arn (hash-ref hashed "Lambda-Runtime-Invoked-Function-Arn")
     'rid (hash-ref hashed "Lambda-Runtime-Aws-Request-Id"))))

(define (next-event-uri)
  (string-append "/" runtime-api-version "/runtime/invocation/next"))
(define (event-response-uri request-id)
  (string-append "/" runtime-api-version "/runtime/invocation/" request-id "/response"))

(define (get-next-event)
  (http-sendrecv
   endpoint
   (next-event-uri)
   #:port port))

(define (event-response ctx outcome)
  (http-sendrecv
   endpoint
   (event-response-uri (hash-ref ctx 'rid))
   #:port port
   #:data (cond
            [(void? outcome) ""]
            [(hash? outcome) (jsexpr->string outcome)]
            [else (~a outcome)])
   #:method "POST"))

(define invoke-handler
  (let ([fn-ns (make-base-namespace)])
    (parameterize ([current-namespace fn-ns])
      (namespace-require 'racket fn-ns)
      (load handler-file))
    (namespace-variable-value handler-name #t #f fn-ns)))

(let loop ()
  (define-values (status headers body)
    (get-next-event))
  (define event (read-json body))
  (write event)
  (define ctx (gen-event-context headers))
  (define outcome (invoke-handler event ctx))
  (event-response ctx outcome)
  (loop))
