// This is free and unencumbered software released into the public domain.
// 
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
// 
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"

	"strings"

	"github.com/apex/log"
	"github.com/apex/log/handlers/cli"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

func main() {

	log.SetHandler(cli.New(os.Stderr))
	log.SetLevel(log.DebugLevel)

	if len(os.Args) != 2 {
		log.Fatalf("supply one filename argument only")
	}

	err := processFile(os.Args[1])
	if err != nil {
		log.Fatalf(err.Error())
	}

}

type influxConfig struct {
	bucket string
	org    string
	token  string
	url    string
}

// Your InfluxDB details go here:
var myInfluxConfig = influxConfig{
	bucket: "",
	org:    "",
	token:  "",
	url:    "",
}

func fixInfluxReservedIdentifier(identifier string) string {
	reserved := []string{
		"ALL", "ALTER", "ANALYZE", "ANY", "AS", "ASC", "BEGIN", "BY", "CREATE",
		"CONTINUOUS", "DATABASE", "DATABASES", "DEFAULT", "DELETE", "DESC",
		"DESTINATIONS", "DIAGNOSTICS", "DISTINCT", "DROP", "DURATION", "END",
		"EVERY", "EXPLAIN", "FIELD", "FOR", "FROM", "GRANT", "GRANTS", "GROUP",
		"GROUPS", "IN", "INF", "INSERT", "INTO", "KEY", "KEYS", "KILL", "LIMIT",
		"SHOW", "MEASUREMENT", "MEASUREMENTS", "NAME", "OFFSET", "ON", "ORDER",
		"PASSWORD", "POLICY", "POLICIES", "PRIVILEGES", "QUERIES", "QUERY",
		"READ", "REPLICATION", "RESAMPLE", "RETENTION", "REVOKE", "SELECT",
		"SERIES", "SET", "SHARD", "SHARDS", "SLIMIT", "SOFFSET", "STATS",
		"SUBSCRIPTION", "SUBSCRIPTIONS", "TAG", "TO", "USER", "USERS",
		"VALUES", "WHERE", "WITH", "WRITE",
	}
	for _, i := range reserved {
		if strings.EqualFold(i, identifier) {
			return strings.ToLower(identifier) + "_"
		}
	}
	return identifier
}

func sendPerfDataToInflux(writeAPI api.WriteAPI, data perfData) error {

	if data.dataType == "HOSTPERFDATA" {
		return nil
	}

	for _, v := range data.perfDataValues {

		var tags = map[string]string{
			"host":  data.hostName,
			"check": fixInfluxReservedIdentifier(v.name),
		}

		errorsCh := writeAPI.Errors()
		go func() {
			for err := range errorsCh {
				log.Errorf("write error: %s", err.Error())
			}
		}()

		var fields = map[string]interface{}{}
		floatValue, err := strconv.ParseFloat(v.value, 32)
		if err != nil {
			log.Errorf("non float perf value: %s", v.value)
			fields = map[string]interface{}{
				"value": v.value,
			}
		} else {
			fields = map[string]interface{}{
				"value": floatValue,
			}
		}

		p := influxdb2.NewPoint(data.serviceDesc, tags, fields, data.timeT)

		writeAPI.WritePoint(p)

	}

	return nil
}

func processFile(path string) error {

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed opening file: %s", err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	client := influxdb2.NewClientWithOptions(
		wmInfluxConfig.url,
		wmInfluxConfig.token,
		influxdb2.DefaultOptions().
			SetHTTPRequestTimeout(600).
			SetMaxRetries(5))

	writeAPI := client.WriteAPI(wmInfluxConfig.org, wmInfluxConfig.bucket)

	for scanner.Scan() {

		perfData, err := getPerfData(scanner.Text())
		if err != nil {
			log.Errorf(err.Error())
		} else {
			sendPerfDataToInflux(writeAPI, perfData)
		}

	}
	client.Close()
	return nil
}

type perfData struct {
	hostPerfData
	servicePerfData
	perfDataValues []perfDataValue
	dataType       string
	timeT          time.Time
	hostName       string
}

type hostPerfData struct {
	hostCheckComand string
	hostState       int
	hostStateType   int
}

type servicePerfData struct {
	serviceDesc         string
	serviceCheckCommand string
	serviceState        int
	serviceType         int
}

type perfDataValue struct {
	name  string
	value string
}

func getPerfData(line string) (perfData, error) {

	var newTokens []string
	tokens := strings.Fields(line)
	for _, token := range tokens {

		fieldNameRegex := regexp.MustCompile(`([A-Z]+::)(.+)`)
		fieldNameMatch := fieldNameRegex.FindAllStringSubmatch(token, -1)
		if fieldNameMatch != nil {
			newTokens = append(newTokens, fieldNameMatch[0][1])
			newTokens = append(newTokens, fieldNameMatch[0][2])
		} else {
			newTokens = append(newTokens, token)
		}
	}

	var tokenJoinBuffer string
	serviceDescParse := false
	servicePerfDataParse := false
	serviceCheckCommandParse := false
	hostPerfDataParse := false
	hostCheckCommandParse := false

	tokens = newTokens
	newTokens = []string{}

	for _, token := range tokens {

		if (serviceDescParse ||
			servicePerfDataParse ||
			serviceCheckCommandParse ||
			hostPerfDataParse ||
			hostCheckCommandParse) && !strings.HasSuffix(token, "::") {
			if tokenJoinBuffer == "" {
				tokenJoinBuffer = token
			} else {
				tokenJoinBuffer = tokenJoinBuffer + " " + token
			}
		} else if (serviceDescParse ||
			servicePerfDataParse ||
			serviceCheckCommandParse ||
			hostPerfDataParse ||
			hostCheckCommandParse) && (strings.HasSuffix(token, "::")) {
			newTokens = append(newTokens, tokenJoinBuffer)
			newTokens = append(newTokens, token)
			serviceDescParse = false
			servicePerfDataParse = false
			serviceCheckCommandParse = false
			hostPerfDataParse = false
			hostCheckCommandParse = false
			tokenJoinBuffer = ""
		} else {
			newTokens = append(newTokens, token)
		}
		if token == "SERVICEDESC::" {
			serviceDescParse = true
		} else if token == "SERVICEPERFDATA::" {
			servicePerfDataParse = true
		} else if token == "SERVICECHECKCOMMAND::" {
			serviceCheckCommandParse = true
		} else if token == "HOSTPERFDATA::" {
			hostPerfDataParse = true
		} else if token == "HOSTCHECKCOMMAND::" {
			hostCheckCommandParse = true
		}

	}

	tokens = newTokens
	prevToken := ""
	skipIdent := false
	var perf = perfData{}

	for _, token := range tokens {

		if skipIdent {
			prevToken = token
			skipIdent = false
			continue
		}

		if strings.HasSuffix(prevToken, "::") {
			skipIdent = true
		}

		switch prevToken {
		case "":
		case "DATATYPE::":
			perf.dataType = token
		case "TIMET::":
			i, err := parseInt(prevToken, token)
			if err != nil {
				log.Errorf(err.Error())
				continue
			} else {
				perf.serviceType = i
			}
			t := time.Unix(int64(i), 0)
			perf.timeT = t
		case "HOSTNAME::":
			perf.hostName = token
		case "SERVICEDESC::":
			perf.serviceDesc = token
		case "SERVICEPERFDATA::", "HOSTPERFDATA::":
			values, err := parsePerfValues(prevToken, token)
			if err != nil {
				return perf, err
			} else {
				perf.perfDataValues = values
			}
		case "SERVICECHECKCOMMAND::":
			perf.serviceCheckCommand = token
		case "SERVICESTATE::":
			i, err := parseInt(prevToken, token)
			if err != nil {
				return perf, err
			} else {
				perf.serviceState = i
			}
		case "SERVICESTATETYPE::":
			i, err := parseInt(prevToken, token)
			if err != nil {
				return perf, err
			} else {
				perf.serviceType = i
			}
		case "HOSTCHECKCOMMAND::":
			perf.hostCheckComand = token
		case "HOSTSTATE::":
			i, err := parseInt(prevToken, token)
			if err != nil {
				return perf, err
			} else {
				perf.hostState = i
			}
		case "HOSTSTATETYPE::":
			i, err := parseInt(prevToken, token)
			if err != nil {
				return perf, err
			} else {
				perf.hostStateType = i
			}
		default:
			return perf, fmt.Errorf("unimplemented perf data field name: %s", prevToken)
		}

		prevToken = token
	}
	return perf, nil
}

func parsePerfValues(token string, rawValue string) ([]perfDataValue, error) {

	var r = []perfDataValue{}
	splitValues := strings.Split(rawValue, " ")

	for _, v := range splitValues {
		perfValueRegex := regexp.MustCompile(`^([A-Za-z0-9-_]+)=(-?[0-9][.0-9]*)[A-Z-a-z%]*;*`)
		perfValueMatch := perfValueRegex.FindAllStringSubmatch(v, -1)
		if perfValueMatch != nil {
			name := perfValueMatch[0][1]
			value := perfValueMatch[0][2]
			p := perfDataValue{
				name:  name,
				value: value,
			}
			r = append(r, p)
		} else {
			return nil, fmt.Errorf("error parsing perfdata value for token: %s, %s", token, v)
		}

	}
	return r, nil
}

func parseInt(token string, rawValue string) (int, error) {
	i, err := strconv.Atoi(rawValue)
	if err != nil {
		return 0, fmt.Errorf("error parsing integer for token: (%s): (%s)", token, rawValue)
	}
	return i, nil
}
