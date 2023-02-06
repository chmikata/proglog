package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	//	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
)

func InitTraceProvider(serviceName, version string) (*sdktrace.TracerProvider, error) {
	traceLogFile, _ := os.CreateTemp("", "traces-*.log")
	log.Printf("trace lo file: %s", traceLogFile.Name())
	f, _ := os.Create(traceLogFile.Name())
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(f),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(serviceName),
				semconv.ServiceVersionKey.String(version),
			),
		),
	)
	return tp, nil
}

func exec(w http.ResponseWriter, r *http.Request) {
	log.Println("exec")
}

func main() {
	ctx := context.Background()
	tp, _ := InitTraceProvider("example-service", "1.0.0")
	defer tp.ForceFlush(ctx)
	//	otel.SetTracerProvider(tp)

	otelOptions := []otelhttp.Option{
		otelhttp.WithTracerProvider(tp),
	}
	otelHandler := otelhttp.NewHandler(http.HandlerFunc(exec), "Hello", otelOptions...)
	http.Handle("/", otelHandler)

	http.ListenAndServe(":3000", nil)
}
