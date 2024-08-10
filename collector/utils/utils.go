// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/blang/semver"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/text/encoding/ianaindex"
	"golang.org/x/text/transform"
	"io/ioutil"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func SafeDereference[T any](s ...*T) []T {
	var resolved []T
	for _, v := range s {
		if v != nil {
			resolved = append(resolved, *v)
		} else {
			var zeroValue T
			resolved = append(resolved, zeroValue)
		}
	}
	return resolved
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if strings.EqualFold(n, x) {
			return true
		}
	}
	return false
}

// parseConstLabels turn param string into prometheus.Labels
func parseConstLabels(s string) prometheus.Labels {
	labels := make(prometheus.Labels)
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil
	}

	parts := strings.Split(s, ",")
	for _, p := range parts {
		keyValue := strings.Split(strings.TrimSpace(p), "=")
		if len(keyValue) != 2 {
			slog.Error(fmt.Sprintf(`malformed labels format %q, should be "key=value"`, p))
			continue
		}
		key := strings.TrimSpace(keyValue[0])
		value := strings.TrimSpace(keyValue[1])
		if key == "" || value == "" {
			continue
		}
		labels[key] = value
	}
	if len(labels) == 0 {
		return nil
	}

	return labels
}

// parseCSV will turn a comma separated string into a []string
func parseCSV(s string) (tags []string) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return nil
	}

	parts := strings.Split(s, ",")
	for _, p := range parts {
		if tag := strings.TrimSpace(p); len(tag) > 0 {
			tags = append(tags, tag)
		}
	}

	if len(tags) == 0 {
		return nil
	}
	return
}

func ParseVersionSem(versionString string) (semver.Version, error) {
	version := parseVersion(versionString)
	if version != "" {
		return semver.ParseTolerant(version)
	}
	return semver.Version{},
		errors.New(fmt.Sprintln("Could not find a openGauss version in string:", versionString))
}

var (
	gaussDBVerRep   = regexp.MustCompile(`(GaussDB|MogDB|Uqbar)\s+Kernel\s+V(\w+)`)
	gaussDBVerRep2  = regexp.MustCompile(`(GaussDB|MogDB|Uqbar)\s+Kernel\s+(\d+\.\d+.\d+)`)
	openGaussVerRep = regexp.MustCompile(`(openGauss|MogDB|Uqbar)\s+(\d+\.\d+.\d+)`)
	vastbaseVerRep  = regexp.MustCompile(`(Vastbase\s+G100)\s+V(\d+\.\d+)`)
)

func parseVersion(versionString string) string {
	versionString = strings.TrimSpace(versionString)
	if gaussDBVerRep.MatchString(versionString) {
		return parseGaussDBVersion(gaussDBVerRep.FindStringSubmatch(versionString))
	}
	if gaussDBVerRep2.MatchString(versionString) {
		return parseOpenGaussVersion(gaussDBVerRep2.FindStringSubmatch(versionString))
	}
	if openGaussVerRep.MatchString(versionString) {
		return parseOpenGaussVersion(openGaussVerRep.FindStringSubmatch(versionString))
	}
	if vastbaseVerRep.MatchString(versionString) {
		return parseVastbaseVersion(vastbaseVerRep.FindStringSubmatch(versionString))
	}
	return ""
}

func parseOpenGaussVersion(subMatches []string) string {
	if len(subMatches) < 3 || subMatches[2] == "" {
		return ""
	}
	return subMatches[2]
}

func parseVastbaseVersion(subMatches []string) string {
	if len(subMatches) < 3 || subMatches[2] == "" {
		return ""
	}
	return subMatches[2]
}

func parseGaussDBVersion(subMatches []string) string {
	if len(subMatches) < 3 || subMatches[2] == "" {
		return ""
	}
	r := regexp.MustCompile(`(\d+)R(\d+)C(\d+)`).FindStringSubmatch(subMatches[2])
	if len(r) < 3 {
		return ""
	}
	r1, _ := strconv.Atoi(r[1])
	r2, _ := strconv.Atoi(r[2])
	r3, _ := strconv.Atoi(r[3])
	return fmt.Sprintf("%v.%v.%v", r1, r2, r3)
}

// Convert database.sql types to float64s for Prometheus consumption. Null types are mapped to NaN. string and []byte
// types are mapped as NaN and !ok
func DbToFloat64(t interface{}) (float64, bool) {
	switch v := t.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case time.Time:
		return float64(v.Unix()), true
	case []byte:
		// Try and convert to string and then parse to a float64
		strV := string(v)
		result, err := strconv.ParseFloat(strV, 64)
		if err != nil {
			slog.Error("Could not parse []byte:", slog.Any("error", err))
			return math.NaN(), false
		}
		return result, true
	case string:
		result, err := strconv.ParseFloat(v, 64)
		if err != nil {
			slog.Error("Could not parse string:", slog.Any("error", err))
			return math.NaN(), false
		}
		return result, true
	case bool:
		if v {
			return 1.0, true
		}
		return 0.0, true
	case nil:
		return math.NaN(), true
	default:
		return math.NaN(), false
	}
}

// Convert database.sql to string for Prometheus labels. Null types are mapped to empty strings.
func DbToString(t interface{}, time2string bool) (string, bool) {
	switch v := t.(type) {
	case int64:
		return fmt.Sprintf("%v", v), true
	case float64:
		return fmt.Sprintf("%v", v), true
	case time.Time:
		if time2string {
			return v.Format(time.RFC3339Nano), true
		}
		return fmt.Sprintf("%v%03d", v.Unix(), v.Nanosecond()/1000000), true
	case nil:
		return "", true
	case []byte:
		// Try and convert to string
		return string(v), true
	case string:
		return v, true
	case bool:
		if v {
			return "true", true
		}
		return "false", true
	default:
		return "", false
	}
}

func RecoverErr(err *error) {
	e := recover()
	switch v := e.(type) {
	case nil:
		// Do nothing
	case error:
		*err = v
	default:
		*err = fmt.Errorf("unexpected error: %#v", e)
	}
}

const (
	UTF8          = "UTF8"
	UTF8Underline = "UTF-8"
	GBK           = "GBK"
	GB18030       = "GB18030"
)

var (
	CharSetMap = map[string]string{
		UTF8:    UTF8Underline,
		GBK:     GBK,
		GB18030: GBK,
	}
)

func GetMapCharset(s string) string {
	o, ok := CharSetMap[strings.ToUpper(s)]
	if ok {
		return o
	}
	return s
}

// DecodeByte 转换为UTF8编码
func DecodeByte(b []byte, charset string) ([]byte, error) {
	charset = GetMapCharset(charset)
	gbkEnc, err := ianaindex.MIB.Encoding(charset)
	if err != nil {
		return b, err
	}
	tmp, err := ioutil.ReadAll(
		transform.NewReader(bytes.NewReader(b), gbkEnc.NewDecoder()),
	)
	if err != nil {
		return b, err
	}
	return tmp, err
}
