package protectedpii

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	defaultKeyID   = "default"
	currentVersion = 1
)

type Value struct {
	Version    int    `json:"version"`
	KeyID      string `json:"keyId"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
	BlindIndex string `json:"blindIndex"`
}

type Protector struct {
	keyID            string
	keyEncryptionKey [32]byte
	globalDataKey    [32]byte
	blindKey         [32]byte
}

type SubjectDataKey struct {
	DataKey    [32]byte
	KeyVersion string
}

func New(encryptionSecret, blindIndexSecret string) (*Protector, error) {
	encryptionSecret = strings.TrimSpace(encryptionSecret)
	blindIndexSecret = strings.TrimSpace(blindIndexSecret)
	if encryptionSecret == "" || blindIndexSecret == "" {
		return nil, errors.New("protected PII secrets are required")
	}
	return &Protector{
		keyID:            defaultKeyID,
		keyEncryptionKey: sha256.Sum256([]byte(encryptionSecret)),
		globalDataKey:    sha256.Sum256([]byte(encryptionSecret + ":global-pii")),
		blindKey:         sha256.Sum256([]byte(blindIndexSecret)),
	}, nil
}

func FromEnv() *Protector {
	encryptionSecret := os.Getenv("PII_KEY_ENCRYPTION_SECRET")
	blindIndexSecret := os.Getenv("PII_BLIND_INDEX_SECRET")
	if encryptionSecret == "" {
		encryptionSecret = "development-pii-key-encryption-secret-change-me"
	}
	if blindIndexSecret == "" {
		blindIndexSecret = "development-pii-blind-index-secret-change-me"
	}
	protector, err := New(encryptionSecret, blindIndexSecret)
	if err != nil {
		panic(err)
	}
	return protector
}

func (p *Protector) Protect(value string) (Value, error) {
	return p.ProtectWithDataKey(value, "", SubjectDataKey{DataKey: p.globalDataKey, KeyVersion: p.keyID})
}

func (p *Protector) ProtectWithDataKey(value, blindIndexField string, subject SubjectDataKey) (Value, error) {
	block, err := aes.NewCipher(subject.DataKey[:])
	if err != nil {
		return Value{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return Value{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return Value{}, err
	}
	ciphertext := aead.Seal(nil, nonce, []byte(value), nil)
	keyID := subject.KeyVersion
	if keyID == "" {
		keyID = p.keyID
	}
	return Value{
		Version:    currentVersion,
		KeyID:      keyID,
		Nonce:      base64.RawURLEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawURLEncoding.EncodeToString(ciphertext),
		BlindIndex: p.BlindIndex(blindIndexField, value),
	}, nil
}

func (p *Protector) MustProtect(value string) Value {
	protected, err := p.Protect(value)
	if err != nil {
		panic(err)
	}
	return protected
}

func (p *Protector) MustProtectWithDataKey(value, blindIndexField string, subject SubjectDataKey) Value {
	protected, err := p.ProtectWithDataKey(value, blindIndexField, subject)
	if err != nil {
		panic(err)
	}
	return protected
}

func (p *Protector) Decrypt(value Value) (string, error) {
	return p.DecryptWithDataKey(value, SubjectDataKey{DataKey: p.globalDataKey, KeyVersion: value.KeyID})
}

func (p *Protector) DecryptWithDataKey(value Value, subject SubjectDataKey) (string, error) {
	if value.Version != currentVersion {
		return "", fmt.Errorf("unsupported protected PII version %d", value.Version)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(value.Nonce)
	if err != nil {
		return "", err
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(value.Ciphertext)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(subject.DataKey[:])
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func (p *Protector) BlindIndex(field, value string) string {
	mac := hmac.New(sha256.New, p.blindKey[:])
	if field != "" {
		_, _ = mac.Write([]byte(field))
		_, _ = mac.Write([]byte{0})
	}
	_, _ = mac.Write([]byte(normalizePiiForBlindIndex(value)))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (p *Protector) SensitiveBlindIndex(field, value string) string {
	mac := hmac.New(sha256.New, p.blindKey[:])
	if field != "" {
		_, _ = mac.Write([]byte(field))
		_, _ = mac.Write([]byte{0})
	}
	_, _ = mac.Write([]byte(value))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func GenerateSubjectDataKey() (SubjectDataKey, error) {
	var key [32]byte
	if _, err := io.ReadFull(rand.Reader, key[:]); err != nil {
		return SubjectDataKey{}, err
	}
	return SubjectDataKey{DataKey: key, KeyVersion: "v1"}, nil
}

func (p *Protector) ProtectSubjectDataKey(subject SubjectDataKey) (Value, error) {
	if subject.KeyVersion == "" {
		subject.KeyVersion = "v1"
	}
	block, err := aes.NewCipher(p.keyEncryptionKey[:])
	if err != nil {
		return Value{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return Value{}, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return Value{}, err
	}
	ciphertext := aead.Seal(nil, nonce, subject.DataKey[:], nil)
	return Value{
		Version:    currentVersion,
		KeyID:      subject.KeyVersion,
		Nonce:      base64.RawURLEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawURLEncoding.EncodeToString(ciphertext),
	}, nil
}

func (p *Protector) UnprotectSubjectDataKey(value Value) (SubjectDataKey, error) {
	if value.Version != currentVersion {
		return SubjectDataKey{}, fmt.Errorf("unsupported subject key version %d", value.Version)
	}
	nonce, err := base64.RawURLEncoding.DecodeString(value.Nonce)
	if err != nil {
		return SubjectDataKey{}, err
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(value.Ciphertext)
	if err != nil {
		return SubjectDataKey{}, err
	}
	block, err := aes.NewCipher(p.keyEncryptionKey[:])
	if err != nil {
		return SubjectDataKey{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return SubjectDataKey{}, err
	}
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return SubjectDataKey{}, err
	}
	if len(plaintext) != 32 {
		return SubjectDataKey{}, errors.New("invalid subject PII data key length")
	}
	var key [32]byte
	copy(key[:], plaintext)
	return SubjectDataKey{DataKey: key, KeyVersion: value.KeyID}, nil
}

func FromEventValue(value any) (Value, bool) {
	if protected, ok := value.(Value); ok {
		return protected, true
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return Value{}, false
	}
	protected := Value{}
	if version, ok := raw["version"].(float64); ok {
		protected.Version = int(version)
	}
	protected.KeyID, _ = raw["keyId"].(string)
	protected.Nonce, _ = raw["nonce"].(string)
	protected.Ciphertext, _ = raw["ciphertext"].(string)
	protected.BlindIndex, _ = raw["blindIndex"].(string)
	return protected, protected.Version != 0 && protected.Nonce != "" && protected.Ciphertext != ""
}

func MustDecryptEventString(protector *Protector, data map[string]any, field string) string {
	protected, ok := FromEventValue(data[field])
	if !ok {
		return ""
	}
	value, err := protector.Decrypt(protected)
	if err != nil {
		panic(err)
	}
	return value
}

func MustDecryptEventStringWithDataKey(protector *Protector, subject SubjectDataKey, data map[string]any, field string) string {
	protected, ok := FromEventValue(data[field])
	if !ok {
		return ""
	}
	value, err := protector.DecryptWithDataKey(protected, subject)
	if err != nil {
		panic(err)
	}
	return value
}

var whitespace = regexp.MustCompile(`\s+`)

func normalizePiiForBlindIndex(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	if !utf8.ValidString(normalized) {
		normalized = strings.ToValidUTF8(normalized, "")
	}
	return whitespace.ReplaceAllString(normalized, " ")
}

// DecryptEventStringWithDataKey rejects malformed encrypted values and returns
// authentication failures to the caller instead of panicking during replay.
func DecryptEventStringWithDataKey(protector *Protector, subject SubjectDataKey, data map[string]any, field string) (string, error) {
	protected, ok := FromEventValue(data[field])
	if !ok {
		return "", fmt.Errorf("invalid protected field %s", field)
	}
	return protector.DecryptWithDataKey(protected, subject)
}
