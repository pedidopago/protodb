package grpce

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"strings"

	"github.com/pedidopago/protodb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const internalPrefix = " ::internal::"

// Error is a struct that bridges a gRPC's grpc.Status error to an Echo (*echo.Echo) error.
type Error struct {
	Code            codes.Code
	Message         string
	InternalMessage string
}

// HTTPError converts a generic error (or status.Status) to a http error
func HTTPError(err error) (httpcode int, msg, internalmsg string) {
	if err == nil {
		return 200, "OK", ""
	}
	if sterr, ok := status.FromError(err); ok {
		switch sterr.Code() {
		case codes.OK:
			return 200, "OK", ""
		case codes.NotFound:
			httpcode = 404
			msg = "Not found"
		case codes.AlreadyExists:
			httpcode = 409
			msg = "Already exists"
		case codes.PermissionDenied:
			httpcode = 403
			msg = "Permission denied"
		case codes.Unauthenticated:
			httpcode = 401
			msg = "Unauthenticated"
		case codes.ResourceExhausted:
			httpcode = 429
			msg = "Resource exhausted"
		case codes.FailedPrecondition:
			httpcode = 400
			msg = "Failed precondition"
		case codes.Aborted:
			httpcode = 409
			msg = "Aborted"
		case codes.OutOfRange:
			httpcode = 400
			msg = "Out of range"
		case codes.Unimplemented:
			httpcode = 501
			msg = "Unimplemented"
		case codes.Internal:
			httpcode = 500
			msg = "Internal error"
		case codes.Unavailable:
			httpcode = 503
			msg = "Unavailable"
		case codes.DataLoss:
			httpcode = 500
			msg = "Data loss"
		default:
			httpcode = 500
			msg = "Unknown error"
		}
		if m := sterr.Message(); m != "" {
			if i := strings.Index(m, internalPrefix); i > -1 {
				msg = m[:i]
				internalmsg = decode(m[i+len(internalPrefix):])
			} else {
				msg = m
			}
		}
		return
	}
	emsg := strings.ToLower(err.Error())
	if strings.Contains(emsg, "internal error") || strings.Contains(emsg, "internal server error") {
		return 500, "internal server error", emsg
	}
	if strings.Contains(emsg, "not found") {
		return 404, "not found", emsg
	}
	if strings.Contains(emsg, strings.ToLower(sql.ErrNoRows.Error())) {
		return 404, "not found", emsg
	}
	return 500, "unknown error", emsg
}

// func (e Error) MarshalErr() error {

// }

const xormsg = "pp2020$%$%53D41359-8423-44AB-B041-EE77DD13F86C"

// encode does a xor operation of msg with xormsg
func encode(msg string) string {
	msg2 := []byte(msg)
	for i := 0; i < len(msg2); i++ {
		msg2[i] ^= xormsg[i%len(xormsg)]
	}
	return base64.RawStdEncoding.EncodeToString(msg2)
}

func decode(msg string) string {
	rawb, err := base64.RawStdEncoding.DecodeString(msg)
	if err != nil {
		println("grpce decode error: " + err.Error())
	}
	msgn := new(bytes.Buffer)
	for i := 0; i < len(rawb); i++ {
		msgn.WriteByte(rawb[i] ^ xormsg[i%len(xormsg)])
	}
	return msgn.String()
}

func StatusError(code codes.Code, message string, internalMessage string) error {
	if internalMessage == "" {
		return status.Error(code, message)
	}
	return status.Error(code, message+internalPrefix+encode(internalMessage))
}

func xdfromctx(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	return "[grpc path:" + strings.Join(md.Get("path"), ";") + "]"
}

func wrap2(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	// if err is already a *status.Status, just return the same error
	if _, ok := status.FromError(err); ok {
		return err
	}

	// otherwise, try to parse the error message
	// and return a *status.Status.Err()

	// sql errors
	if protodb.IsNotFound(err) {
		nfe := err.(*protodb.NotFoundError)
		return StatusError(codes.NotFound, nfe.Name, xdfromctx(ctx))
	}
	if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
		return StatusError(codes.NotFound, strings.Replace(err.Error(), sql.ErrNoRows.Error(), "", -1), xdfromctx(ctx))
	}
	if protodb.IsQueryError(err) {
		qerr := err.(*protodb.QueryError)
		if qerr.Err != nil {
			return StatusError(codes.Internal, qerr.Message, xdfromctx(ctx)+" query(id): "+qerr.Query+"; "+qerr.Err.Error())
		}
		return StatusError(codes.Internal, qerr.Message, xdfromctx(ctx)+" query(id): "+qerr.Query)
	}
	//TODO: try parse more common errors
	return StatusError(codes.Internal, "internal error", xdfromctx(ctx)+" "+err.Error())
}

func Wrap(err error) error {
	return wrap2(nil, err)
}

func WrapRPC(ctx context.Context, fn func() error) error {
	return wrap2(ctx, fn())
}
