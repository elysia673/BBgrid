package register

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
)

//type RegisterRequest struct {
//	ClientID  string `json:"client_id"`
//	PublicKey string `json:"public_key"`
//	Token     string `json:"token"`
//}
//
//type RegisterResponse struct {
//	Code int    `json:"code"`
//	Msg  string `json:"msg"`
//	Data struct {
//		Certificate string `json:"certificate"`
//	} `json:"data"`
//}

// GenerateKeyPair 生成 ECC 密钥对并保存到文件
// privateKeyPath: 私钥保存路径（client.key）
// publicKeyPath: 公钥保存路径（client.pub）
func GenerateKeyPair(privateKeyPath, publicKeyPath string) error {
	// 生成 私钥
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
	privateFile, _ := os.Create(privateKeyPath)
	defer privateFile.Close()

	err = pem.Encode(privateFile, &pem.Block{
		Type:  "EC PRIVATE KEY", // PEM 头部标记
		Bytes: privateBytes,     // DER 编码私钥数据
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
		Type:  "PUBLIC KEY", // PEM 头部标记
		Bytes: publicBytes,  // DER 编码公钥数据
	})
	if err != nil {
		return err
	}

	return nil
}

//func Register(serverURL, clientID, token, publicKeyPath, certPath string) error {
//	publicKeyData, err := os.ReadFile(publicKeyPath)
//	if err != nil {
//		return err
//	}
//
//	reqBody := RegisterRequest{
//		ClientID:  clientID,
//		PublicKey: string(publicKeyData),
//		Token:     token,
//	}
//
//	bodyBytes, _ := json.Marshal(reqBody)
//
//	tr := &http.Transport{
//		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
//	}
//
//	client := &http.Client{Transport: tr}
//
//	resp, err := client.Post(serverURL+"/api/v1/register", "application/json", bytes.NewBuffer(bodyBytes))
//	if err != nil {
//		return err
//	}
//	defer resp.Body.Close()
//
//	respBody, _ := io.ReadAll(resp.Body)
//
//	var result RegisterResponse
//	json.Unmarshal(respBody, &result)
//
//	if result.Code != 0 {
//		return fmt.Errorf("注册失败: %s", result.Msg)
//	}
//
//	err = os.WriteFile(certPath, []byte(result.Data.Certificate), 0600)
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
