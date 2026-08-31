package workflows

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/altipla-consulting/errors"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	jsonMarshalerType   = reflect.TypeFor[json.Marshaler]()
	jsonUnmarshalerType = reflect.TypeFor[json.Unmarshaler]()
)

func collectProtobufs(value any) (map[string]json.RawMessage, error) {
	protobufs := make(map[string]json.RawMessage)
	visit := func(path []string, message proto.Message) error {
		raw, err := protojson.Marshal(message)
		if err != nil {
			return errors.Trace(err)
		}
		protobufs[pathKey(path)] = raw
		return nil
	}
	if err := walkValue(reflect.ValueOf(value), nil, jsonMarshalerType, visit); err != nil {
		return nil, errors.Trace(err)
	}
	return protobufs, nil
}

func restoreProtobufs(value any, protobufs map[string]json.RawMessage) error {
	if len(protobufs) == 0 {
		return nil
	}

	restored := make(map[string]bool, len(protobufs))
	visit := func(path []string, message proto.Message) error {
		key := pathKey(path)
		raw, ok := protobufs[key]
		if !ok {
			return fmt.Errorf("protobuf return value at %s changed shape", formatPath(path))
		}
		if err := protojson.Unmarshal(raw, message); err != nil {
			return errors.Trace(err)
		}
		restored[key] = true
		return nil
	}
	if err := walkValue(reflect.ValueOf(value), nil, jsonUnmarshalerType, visit); err != nil {
		return errors.Trace(err)
	}
	for key := range protobufs {
		if !restored[key] {
			return fmt.Errorf("protobuf return value at %s cannot be restored into its declared type", formatPathKey(key))
		}
	}
	return nil
}

func walkValue(value reflect.Value, path []string, jsonInterface reflect.Type, visit func([]string, proto.Message) error) error {
	if !value.IsValid() {
		return nil
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil
		}
		return walkValue(value.Elem(), path, jsonInterface, visit)
	}
	if value.CanInterface() {
		if message, ok := value.Interface().(proto.Message); ok {
			if value.Kind() == reflect.Pointer && value.IsNil() {
				return nil
			}
			return visit(path, message)
		}
		if value.Type().Implements(jsonInterface) {
			return nil
		}
	}

	switch value.Kind() {
	case reflect.Pointer:
		if !value.IsNil() {
			return walkValue(value.Elem(), path, jsonInterface, visit)
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if field.PkgPath != "" || name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if err := walkValue(value.Field(i), appendPath(path, name), jsonInterface, visit); err != nil {
				return errors.Trace(err)
			}
		}
	case reflect.Array, reflect.Slice:
		for i := 0; i < value.Len(); i++ {
			if err := walkValue(value.Index(i), appendPath(path, "["+strconv.Itoa(i)+"]"), jsonInterface, visit); err != nil {
				return errors.Trace(err)
			}
		}
	case reflect.Map:
		if value.IsNil() {
			return nil
		}
		for _, key := range value.MapKeys() {
			name, err := mapKey(key)
			if err != nil {
				return errors.Trace(err)
			}
			if err := walkValue(value.MapIndex(key), appendPath(path, name), jsonInterface, visit); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func mapKey(key reflect.Value) (string, error) {
	raw, err := json.Marshal(key.Interface())
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(raw), nil
}

func appendPath(path []string, part string) []string {
	return append(append([]string(nil), path...), part)
}

func pathKey(path []string) string {
	raw, _ := json.Marshal(path)
	return string(raw)
}

func formatPathKey(key string) string {
	var path []string
	if err := json.Unmarshal([]byte(key), &path); err != nil {
		return key
	}
	return formatPath(path)
}

func formatPath(path []string) string {
	if len(path) == 0 {
		return "root"
	}
	return strings.Join(path, "/")
}
