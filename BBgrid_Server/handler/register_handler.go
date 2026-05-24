package handler

import (
	"BBgrid/BBgrid_Server/register"
	"BBgrid/common/config"
	"BBgrid/common/model"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

type RegisterHandler struct {
	cfg      *config.ServerConfig
	registry *register.Registry
}

func NewRegisterHandler(cfg *config.ServerConfig, registry *register.Registry) *RegisterHandler {
	return &RegisterHandler{cfg: cfg, registry: registry}
}

type RegisterApplyRequest struct {
	ClientID  string `json:"client_id" binding:"required"`
	PublicKey string `json:"public_key" binding:"required"`
	Token     string `json:"token" binding:"required"`
}

// HandleRegisterApply 提交注册申请
func (h *RegisterHandler) HandleRegisterApply(c *gin.Context) {
	var req RegisterApplyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(1, "invalid request"))
		return
	}

	// 验证 token
	if req.Token != h.cfg.Auth.ClientToken {
		c.JSON(http.StatusUnauthorized, model.Error(1, "invalid token"))
		return
	}

	// 添加申请
	if err := h.registry.AddApplication(req.ClientID, req.PublicKey); err != nil {
		c.JSON(http.StatusConflict, model.Error(1, err.Error()))
		return
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"message":   "application submitted",
		"client_id": req.ClientID,
	}))
}

type RegisterAddRequest struct {
	ClientID string `json:"client_id" binding:"required"`
}

// HandleRegisterAdd 审核通过并签发证书
func (h *RegisterHandler) HandleRegisterAdd(c *gin.Context) {
	var req RegisterAddRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(1, "invalid request"))
		return
	}

	// 审核通过
	certPEM, err := h.registry.Approve(req.ClientID)
	if err != nil {
		c.JSON(http.StatusBadRequest, model.Error(1, err.Error()))
		return
	}

	// 去掉证书头部和换行符，只保留实际内容
	certContent := string(certPEM)
	certContent = strings.ReplaceAll(certContent, "-----BEGIN CERTIFICATE-----", "")
	certContent = strings.ReplaceAll(certContent, "-----END CERTIFICATE-----", "")
	certContent = strings.ReplaceAll(certContent, "\n", "")
	certContent = strings.TrimSpace(certContent)

	certPrefix := certContent
	if len(certPrefix) > 40 {
		certPrefix = certPrefix[:40]
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"certificate": string(certPEM),
		"cert_prefix": certPrefix,
		"client_id":   req.ClientID,
	}))
}

type RegisterDeleteRequest struct {
	ClientID   string `json:"client_id" binding:"required"`
	CertPrefix string `json:"cert_prefix" binding:"required"`
}

// HandleRegisterDelete 吊销客户端证书
func (h *RegisterHandler) HandleRegisterDelete(c *gin.Context) {
	var req RegisterDeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.Error(1, "invalid request"))
		return
	}

	record := h.registry.GetByClientID(req.ClientID)
	if record == nil {
		c.JSON(http.StatusNotFound, model.Error(1, "client not found"))
		return
	}

	// 验证证书前缀
	actualCert := record.Certificate
	actualCert = strings.ReplaceAll(actualCert, "-----BEGIN CERTIFICATE-----", "")
	actualCert = strings.ReplaceAll(actualCert, "-----END CERTIFICATE-----", "")
	actualCert = strings.ReplaceAll(actualCert, "\n", "")
	actualCert = strings.TrimSpace(actualCert)

	actualPrefix := actualCert
	if len(actualPrefix) > 40 {
		actualPrefix = actualPrefix[:40]
	}
	if actualPrefix != req.CertPrefix {
		c.JSON(http.StatusUnauthorized, model.Error(1, "certificate prefix mismatch"))
		return
	}

	if !h.registry.Delete(req.ClientID) {
		c.JSON(http.StatusInternalServerError, model.Error(1, "delete failed"))
		return
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"message": "client removed",
	}))
}

// HandleRegisterApplyList 获取待审核列表
func (h *RegisterHandler) HandleRegisterApplyList(c *gin.Context) {
	records := h.registry.GetPending()

	type ApplyInfo struct {
		ClientID  string `json:"client_id"`
		CreatedAt int64  `json:"created_at"`
	}

	var applies []ApplyInfo
	for _, record := range records {
		applies = append(applies, ApplyInfo{
			ClientID:  record.ClientID,
			CreatedAt: record.CreatedAt,
		})
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"applications": applies,
	}))
}

// HandleRegisterInfo 获取已通过列表
func (h *RegisterHandler) HandleRegisterInfo(c *gin.Context) {
	records := h.registry.GetApproved()

	type ClientInfo struct {
		ClientID    string `json:"client_id"`
		CertPrefix  string `json:"cert_prefix"`
		Certificate string `json:"certificate,omitempty"`
		ApprovedAt  int64  `json:"approved_at"`
	}

	var clients []ClientInfo
	for _, record := range records {
		certContent := record.Certificate
		certContent = strings.ReplaceAll(certContent, "-----BEGIN CERTIFICATE-----", "")
		certContent = strings.ReplaceAll(certContent, "-----END CERTIFICATE-----", "")
		certContent = strings.ReplaceAll(certContent, "\n", "")
		certContent = strings.TrimSpace(certContent)

		prefix := certContent
		if len(prefix) > 40 {
			prefix = prefix[:40]
		}
		clients = append(clients, ClientInfo{
			ClientID:    record.ClientID,
			CertPrefix:  prefix,
			Certificate: record.Certificate,
			ApprovedAt:  record.ApprovedAt,
		})
	}

	c.JSON(http.StatusOK, model.Success(gin.H{
		"clients": clients,
	}))
}
