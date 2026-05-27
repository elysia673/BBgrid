package register

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
)

// GenerateKeyPair 生成 ECC 密钥对并保存到文件
// privateKeyPath: 私钥保存路径（client.key）
// publicKeyPath: 公钥保存路径（client.pub）
func GenerateKeyPair(privateKeyPath, publicKeyPath string) error {
	// 生成 ECC P-256 私钥
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}

	// 私钥转化为 DER 格式（二进制）
	privateBytes, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return err
	}

	// 转换为 PEM 格式（文本格式，便于存储和传输）
	privateFile, err := os.Create(privateKeyPath)
	if err != nil {
		return err
	}
	defer privateFile.Close()

	err = pem.Encode(privateFile, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: privateBytes,
	})
	if err != nil {
		return err
	}

	// 提取公钥并序列化
	publicBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return err
	}

	// 保存公钥为 PEM 格式
	publicFile, err := os.Create(publicKeyPath)
	if err != nil {
		return err
	}
	defer publicFile.Close()

	err = pem.Encode(publicFile, &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicBytes,
	})
	if err != nil {
		return err
	}

	return nil
}
