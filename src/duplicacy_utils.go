// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gilbertchen/gopass"
	"golang.org/x/crypto/pbkdf2"
)

var RunInBackground bool = false

type RateLimitedReader struct {
	Content   []byte
	Rate      float64
	Next      int
	StartTime time.Time
}

var RegexMap map[string]*regexp.Regexp

func init() {

	if RegexMap == nil {
		RegexMap = make(map[string]*regexp.Regexp)
	}

}

func CreateRateLimitedReader(content []byte, rate int) *RateLimitedReader {
	return &RateLimitedReader{
		Content: content,
		Rate:    float64(rate * 1024),
		Next:    0,
	}
}

func IsEmptyFilter(pattern string) bool {
	if pattern == "+" || pattern == "-" || pattern == "i:" || pattern == "e:" {
		return true
	} else {
		return false
	}
}

func IsUnspecifiedFilter(pattern string) bool {
	if pattern[0] != '+' && pattern[0] != '-' && pattern[0] != 'i' && pattern[0] != 'e' {
		return true
	} else {
		return false
	}
}

func IsValidRegex(pattern string) (valid bool, err error) {

	var re *regexp.Regexp = nil

	if re, valid = RegexMap[pattern]; valid && re != nil {
		return true, nil
	}

	re, err = regexp.Compile(pattern)

	if err != nil {
		return false, err
	} else {
		RegexMap[pattern] = re
		LOG_DEBUG("REGEX_STORED", "Saved compiled regex for pattern \"%s\", regex=%#v", pattern, re)
		return true, err
	}
}

func (reader *RateLimitedReader) Length() int64 {
	return int64(len(reader.Content))
}

func (reader *RateLimitedReader) Reset() {
	reader.Next = 0
}

func (reader *RateLimitedReader) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekStart {
		reader.Next = int(offset)
	} else if whence == io.SeekCurrent {
		reader.Next += int(offset)
	} else {
		reader.Next = len(reader.Content) - int(offset)
	}
	return int64(reader.Next), nil
}

func (reader *RateLimitedReader) Read(p []byte) (n int, err error) {

	if reader.Next >= len(reader.Content) {
		return 0, io.EOF
	}

	if reader.Rate <= 0 {
		n := copy(p, reader.Content[reader.Next:])
		reader.Next += n
		if reader.Next >= len(reader.Content) {
			return n, io.EOF
		}
		return n, nil
	}

	if reader.StartTime.IsZero() {
		reader.StartTime = time.Now()
	}

	elapsed := time.Since(reader.StartTime).Seconds()
	delay := float64(reader.Next)/reader.Rate - elapsed
	end := reader.Next + int(reader.Rate/5)
	if delay > 0 {
		time.Sleep(time.Duration(delay * float64(time.Second)))
	} else {
		end += -int(delay * reader.Rate)
	}

	if end > len(reader.Content) {
		end = len(reader.Content)
	}

	n = copy(p, reader.Content[reader.Next:end])
	reader.Next += n
	return n, nil
}

func RateLimitedCopy(writer io.Writer, reader io.Reader, rate int) (written int64, err error) {
	if rate <= 0 {
		return io.Copy(writer, reader)
	}
	for range time.Tick(time.Second / 5) {
		n, err := io.CopyN(writer, reader, int64(rate*1024/5))
		written += n
		if err != nil {
			if err == io.EOF {
				return written, nil
			} else {
				return written, err
			}
		}
	}
	return written, nil
}

// GenerateKeyFromPassword generates a key from the password.
func GenerateKeyFromPassword(password string, salt []byte, iterations int) []byte {
	return pbkdf2.Key([]byte(password), salt, iterations, 32, sha256.New)
}

// Get password from preference, env, but don't start any keyring request
func GetPasswordFromPreference(preference Preference, passwordType string) string {
	passwordID := passwordType
	if preference.Name != "default" {
		passwordID = preference.Name + "_" + passwordID
	}

	{
		name := strings.ToUpper("duplicacy_" + passwordID)
		LOG_DEBUG("PASSWORD_ENV_VAR", "Reading the environment variable %s", name)
		if password, found := os.LookupEnv(name); found && password != "" {
			return password
		}
	}

	// If the password is stored in the preference, there is no need to include the storage name
	// (i.e., preference.Name) in the key, so the key name should really be passwordType rather
	// than passwordID; we're using passwordID here only for backward compatibility
	if len(preference.Keys) > 0 && len(preference.Keys[passwordID]) > 0 {
		LOG_DEBUG("PASSWORD_PREFERENCE", "Reading %s from preferences", passwordID)
		return preference.Keys[passwordID]
	}

	if len(preference.Keys) > 0 && len(preference.Keys[passwordType]) > 0 {
		LOG_DEBUG("PASSWORD_PREFERENCE", "Reading %s from preferences", passwordType)
		return preference.Keys[passwordType]
	}

	return ""
}

// GetPassword attempts to get the password from KeyChain/KeyRing, environment variables, or keyboard input.
func GetPassword(preference Preference, passwordType string, prompt string,
	showPassword bool, resetPassword bool) string {
	passwordID := passwordType

	preferencePassword := GetPasswordFromPreference(preference, passwordType)
	if preferencePassword != "" {
		return preferencePassword
	}

	if preference.Name != "default" {
		passwordID = preference.Name + "_" + passwordID
	}

	if resetPassword && !RunInBackground {
		keyringSet(passwordID, "")
	} else {
		password := keyringGet(passwordID)
		if password != "" {
			LOG_DEBUG("PASSWORD_KEYCHAIN", "Reading %s from keychain/keyring", passwordType)
			return password
		}

		if RunInBackground {
			LOG_INFO("PASSWORD_MISSING", "%s is not found in Keychain/Keyring", passwordID)
			return ""
		}

	}

	password := ""
	fmt.Printf("%s", prompt)
	if showPassword {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		password = scanner.Text()
	} else {
		passwordInBytes, err := gopass.GetPasswdMasked()
		if err != nil {
			LOG_ERROR("PASSWORD_READ", "Failed to read the password: %v", err)
			return ""
		}
		password = string(passwordInBytes)
	}

	return password
}

// SavePassword saves the specified password in the keyring/keychain.
func SavePassword(preference Preference, passwordType string, password string) {

	if password == "" || RunInBackground {
		return
	}

	if preference.DoNotSavePassword {
		return
	}

	// If the password is retrieved from env or preference, don't save it to keyring
	if GetPasswordFromPreference(preference, passwordType) == password {
		return
	}

	passwordID := passwordType
	if preference.Name != "default" {
		passwordID = preference.Name + "_" + passwordID
	}
	keyringSet(passwordID, password)
}

// The following code was modified from the online article  'Matching Wildcards: An Algorithm', by Kirk J. Krauss,
// Dr. Dobb's, August 26, 2008. However, the version in the article doesn't handle cases like matching 'abcccd'
// against '*ccd', and the version here fixed that issue.
//
func matchPattern(text string, pattern string) bool {

	textLength := len(text)
	patternLength := len(pattern)
	afterLastWildcard := 0
	afterLastMatched := 0

	t := 0
	p := 0

	for {
		if t >= textLength {
			if p >= patternLength {
				return true // "x" matches "x"
			} else if pattern[p] == '*' {
				p++
				continue // "x*" matches "x" or "xy"
			}
			return false // "x" doesn't match "xy"
		}

		w := byte(0)
		if p < patternLength {
			w = pattern[p]
		}

		if text[t] != w {
			if w == '?' {
				t++
				p++
				continue
			} else if w == '*' {
				p++
				afterLastWildcard = p
				if p >= patternLength {
					return true
				}
			} else if afterLastWildcard > 0 {
				p = afterLastWildcard
				t = afterLastMatched
				t++
			} else {
				return false
			}

			for t < textLength && text[t] != pattern[p] && pattern[p] != '?' {
				t++
			}

			if t >= textLength {
				return false
			}
			afterLastMatched = t
		}
		t++
		p++
	}

}

// MatchPathVerbose returns 'true' if the file 'filePath' is included by the specified 'patterns' or 'false'
// if it is excluded, along with with the pattern responsible for the inclusion or exclusion.  If the file
// was included or excluded by default (i.e. not as the result of a pattern), an empty string is returned as
// the pattern.  Each pattern starts with either '+' or '-', whereas '-' indicates exclusion and '+' indicates
// inclusion.  Wildcards like '*' and '?' may appear in the patterns.  In case no matching pattern is found,
// the file will be excluded if all patterns are include patterns, and included otherwise.
func MatchPathVerbose(filePath string, patterns []string) (included bool, pattern string) {

	var re *regexp.Regexp = nil
	var found bool
	var matched bool

	allIncludes := true

	for _, pattern := range patterns {
		if pattern[0] == '+' {
			if matchPattern(filePath, pattern[1:]) {
				return true, pattern
			}
		} else if pattern[0] == '-' {
			allIncludes = false
			if matchPattern(filePath, pattern[1:]) {
				return false, pattern
			}
		} else if strings.HasPrefix(pattern, "i:") || strings.HasPrefix(pattern, "e:") {
			if re, found = RegexMap[pattern[2:]]; found {
				matched = re.MatchString(filePath)
			} else {
				re, err := regexp.Compile(pattern)
				if err != nil {
					LOG_ERROR("REGEX_ERROR", "Invalid regex encountered for pattern \"%s\" - %v", pattern[2:], err)
				}
				RegexMap[pattern] = re
				matched = re.MatchString(filePath)
			}
			if matched {
				return strings.HasPrefix(pattern, "i:"), pattern
			} else {
				if strings.HasPrefix(pattern, "e:") {
					allIncludes = false
				}
			}
		}
	}

	return !allIncludes, ""
}

// MatchPath returns 'true' if the file 'filePath' is included by the specified 'patterns'.  Each pattern starts with
// either '+' or '-', whereas '-' indicates exclusion and '+' indicates inclusion.  Wildcards like '*' and '?' may
// appear in the patterns.  In case no matching pattern is found, the file will be excluded if all patterns are
// include patterns, and included otherwise.
func MatchPath(filePath string, patterns []string) (included bool) {
    included, _ = MatchPathVerbose(filePath, patterns)
    return included
}

func joinPath(components ...string) string {

	combinedPath := path.Join(components...)
	if len(combinedPath) > 257 && runtime.GOOS == "windows" {
		combinedPath = `\\?\` + filepath.Join(components...)
		// If the path is on a samba drive we must use the UNC format
		if strings.HasPrefix(combinedPath, `\\?\\\`) {
			combinedPath = `\\?\UNC\` + combinedPath[6:]
		}
	}
	return combinedPath
}

func PrettyNumber(number int64) string {

	G := int64(1024 * 1024 * 1024)
	M := int64(1024 * 1024)
	K := int64(1024)

	if number > 1000*G {
		return fmt.Sprintf("%dG", number/G)
	} else if number > G {
		return fmt.Sprintf("%d,%03dM", number/(1000*M), (number/M)%1000)
	} else if number > M {
		return fmt.Sprintf("%d,%03dK", number/(1000*K), (number/K)%1000)
	} else if number > K {
		return fmt.Sprintf("%dK", number/K)
	} else {
		return fmt.Sprintf("%d", number)
	}
}

func PrettySize(size int64) string {
	if size > 1024*1024 {
		return fmt.Sprintf("%.2fM", float64(size)/(1024.0*1024.0))
	} else if size > 1024 {
		return fmt.Sprintf("%.0fK", float64(size)/1024.0)
	} else {
		return fmt.Sprintf("%d", size)
	}
}

func PrettyTime(seconds int64) string {

	day := int64(3600 * 24)

	if seconds > day*2 {
		return fmt.Sprintf("%d days %02d:%02d:%02d",
			seconds/day, (seconds%day)/3600, (seconds%3600)/60, seconds%60)
	} else if seconds > day {
		return fmt.Sprintf("1 day %02d:%02d:%02d", (seconds%day)/3600, (seconds%3600)/60, seconds%60)
	} else if seconds > 0 {
		return fmt.Sprintf("%02d:%02d:%02d", seconds/3600, (seconds%3600)/60, seconds%60)
	} else {
		return "n/a"
	}
}

func AtoSize(sizeString string) int {
	sizeString = strings.ToLower(sizeString)

	sizeRegex := regexp.MustCompile(`^([0-9]+)([mk])?$`)
	matched := sizeRegex.FindStringSubmatch(sizeString)
	if matched == nil {
		return 0
	}

	size, _ := strconv.Atoi(matched[1])

	if matched[2] == "m" {
		size *= 1024 * 1024
	} else if matched[2] == "k" {
		size *= 1024
	}

	return size
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}
