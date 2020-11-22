#lang racket/base

(provide hello-world
         hello-error)

(define (hello-world ctx) "hello world")

(define (hello-error ctx)
  (error 'oops "uh-oh"))

