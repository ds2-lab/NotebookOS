package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"

	configKit "github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yaml"
	"github.com/mitchellh/mapstructure"
)

const (
	OptionName = "name"
	OptionDesc = "description"
)

var (
	Flag          *flag.FlagSet = flag.NewFlagSet("", flag.ContinueOnError)
	ErrNonPointer               = errors.New("validate with non-pointer")
	ErrPrintUsage               = errors.New("print usage")

	zeroValue = reflect.Value{}
)

// Options is the interface for all config options.
// See the package-level substructs to define your options.
type Options interface {
	// Validate validates the options.
	Validate() error

	// init initializes the options based on defined tags.
	init(opts interface{}) error

	// meta returns underline options contains meta info.
	meta() *options
}

type options struct {
	YAML string `name:"yaml" description:"Path to config file in the yml format."`

	root Options
	seen map[reflect.Type]interface{}
	raw  reflect.Value
}

func NewOptions() Options {
	root := &options{seen: make(map[reflect.Type]interface{})}
	root.root = root
	return root
}

func Polyfill(opts Options, fill Options) error {
	val := reflect.ValueOf(opts)
	if val.Kind() != reflect.Ptr {
		return ErrNonPointer
	}

	oType := reflect.TypeOf(opts).Elem()
	oVal := reflect.ValueOf(opts).Elem()
	if oType == reflect.TypeOf(options{}) {
		return nil
	}

	root := oVal.FieldByName("Options")
	if root != zeroValue && root.IsNil() {
		if fill == nil {
			fill = NewOptions()
		}
		root.Set(reflect.ValueOf(fill))
	}
	return nil
}

// ValidateOptions validates the options with command line arguments.
// Returns a FlagSet and error.
// If returns ErrPrintUsage, the usage should be printed.
func ValidateOptions(opts Options) (*flag.FlagSet, error) {
	return ValidateOptionsWithFlags(opts, os.Args[1:]...)
}

// ValidateOptionsWithFlags validates the options with specified arguments.
// Returns a FlagSet and error.
// If returns ErrPrintUsage, the usage should be printed.
func ValidateOptionsWithFlags(opts Options, args ...string) (*flag.FlagSet, error) {
	var printInfo bool
	if Flag.Parsed() {
		Flag = flag.NewFlagSet(Flag.Name(), Flag.ErrorHandling())
	}
	Flag.BoolVar(&printInfo, "h", false, "Show help.")

	if err := Polyfill(opts, nil); err != nil {
		return Flag, err
	}
	if err := opts.init(opts); err != nil {
		return Flag, err
	}

	err := Flag.Parse(args)
	if err == nil && printInfo {
		err = ErrPrintUsage
	}
	if err != nil {
		return Flag, err
	}

	meta := opts.meta()
	for _, s := range meta.seen {
		if opts, ok := s.(Options); ok {
			if err := opts.Validate(); err != nil {
				return Flag, err
			}
		}
	}

	return Flag, nil
}

func (o *options) init(opts interface{}) error {
	t := reflect.TypeOf(opts)
	defer func() {
		o.seen[t] = opts
		// log.Printf("seen %v", t)
	}()

	oType := t.Elem()
	oVal := reflect.ValueOf(opts).Elem()
	if o.raw == zeroValue {
		o.raw = oVal
	}
	numField := oType.NumField()
	for i := 0; i < numField; i++ {
		field := oType.Field(i)
		if field.PkgPath != "" {
			continue
		}

		// Recursively check embedded options except the "root".
		opt := oVal.Field(i)
		if opt.Kind() != reflect.Interface && opt.Kind() != reflect.Ptr {
			opt = opt.Addr()
		}
		if opt.CanInterface() {
			_, seen := o.seen[opt.Type()]
			// log.Printf("checking %s, type %v, seen %v, kind %v", field.Name, opt.Type(), seen, field.Type.Kind())
			if seen {
				continue
			}

			if innerOpts, ok := opt.Interface().(Options); ok {
				if err := Polyfill(innerOpts, o.root); err != nil {
					return err
				}
				if err := innerOpts.init(innerOpts); err != nil {
					return err
				}
				// Make sure the Options interface is seen too.
				if _, seen := o.seen[opt.Type()]; !seen {
					o.seen[opt.Type()] = innerOpts
					// log.Printf("seen %v", opt.Type())
				}
				continue
			} else if field.Type.Kind() == reflect.Struct {
				if err := o.init(opt.Interface()); err != nil {
					return err
				}
				continue
			}
			// else: pass
		}

		name := field.Tag.Get(OptionName)
		if name == "" {
			continue
		}
		desc := field.Tag.Get(OptionDesc)
		switch field.Type.Kind() {
		case reflect.Bool:
			Flag.BoolVar(opt.Interface().(*bool), name, opt.Elem().Bool(), desc)
		case reflect.Int:
			Flag.IntVar(opt.Interface().(*int), name, int(opt.Elem().Int()), desc)
		case reflect.Int64:
			Flag.Int64Var(opt.Interface().(*int64), name, opt.Elem().Int(), desc)
		case reflect.Uint:
			Flag.UintVar(opt.Interface().(*uint), name, uint(opt.Elem().Uint()), desc)
		case reflect.Uint64:
			Flag.Uint64Var(opt.Interface().(*uint64), name, opt.Elem().Uint(), desc)
		case reflect.Float64:
			Flag.Float64Var(opt.Interface().(*float64), name, opt.Elem().Float(), desc)
		case reflect.String:
			Flag.StringVar(opt.Interface().(*string), name, opt.Elem().String(), desc)
		default:
			return fmt.Errorf("unsupprted config type: %v(%s)", field.Type.Kind(), field.Name)
		}
	}

	return nil
}

func (o *options) meta() *options {
	return o
}

func (o *options) Validate() error {
	if o.YAML != "" {
		yml := o.YAML
		o.YAML = ""

		config := configKit.NewWithOptions("", func(opt *configKit.Options) {
			opt.TagName = OptionName
			// DecoderConfig initialization is due a bug in configKit: no TagName will be applied if DecoderConfig is nil.
			// TODO: Fix the bug
			opt.DecoderConfig = &mapstructure.DecoderConfig{}
		})
		config.AddDriver(yaml.Driver)

		if err := config.LoadFiles(yml); err != nil {
			return err
		}

		// fileOpts := reflect.New(o.raw.Type())
		// if err := config.BindStruct("", fileOpts.Interface()); err != nil {
		// 	return err
		// }

		// Merge the options by flags set.
		data := config.Data()
		for k, v := range data {
			flag := Flag.Lookup(k)
			if flag == nil || flag.Value.String() != flag.DefValue {
				continue
			}

			err := flag.Value.Set(fmt.Sprintf("%v", v))
			if err != nil {
				return fmt.Errorf("invalid value \"%s\" for \"%s\": %v", v, flag.Name, err)
			}
		}
		// if err := mergo.Merge(o.raw.Addr().Interface(), fileOpts.Interface()); err != nil {
		// 	return err
		// }
	}

	return nil
}
