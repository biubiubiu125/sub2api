package routes

import (
	"net/http"
	"path/filepath"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/setup"

	"github.com/gin-gonic/gin"
)

func RegisterReferralRoutes(
	r *gin.Engine,
	v1 *gin.RouterGroup,
	h *handler.Handlers,
	jwtAuth middleware.JWTAuthMiddleware,
	adminAuth middleware.AdminAuthMiddleware,
	settingService *service.SettingService,
) {
	if h == nil || h.Referral == nil || h.Admin == nil || h.Admin.Referral == nil {
		return
	}

	r.StaticFS("/referral-assets", http.Dir(filepath.Join(setup.GetDataDir(), "public", "referral-assets")))
	r.GET("/r/:code", h.Referral.CaptureReferral)

	authenticated := v1.Group("")
	authenticated.Use(gin.HandlerFunc(jwtAuth))
	authenticated.Use(middleware.BackendModeUserGuard(settingService))
	{
		ext := authenticated.Group("/ext")
		{
			referral := ext.Group("/referral")
			{
				referral.GET("/summary", h.Referral.GetSummary)
				referral.GET("/commissions", h.Referral.ListCommissions)
				referral.GET("/withdrawals", h.Referral.ListWithdrawals)
				referral.POST("/withdrawals", h.Referral.CreateWithdrawal)
				referral.POST("/withdrawals/:id/cancel", h.Referral.CancelWithdrawal)
				referral.POST("/upload", h.Referral.UploadAsset)
			}
		}
	}

	admin := v1.Group("/admin")
	admin.Use(gin.HandlerFunc(adminAuth))
	{
		referral := admin.Group("/referral")
		{
			referral.GET("/overview", h.Admin.Referral.Overview)
			referral.GET("/settings", h.Admin.Referral.GetSettings)
			referral.PUT("/settings", h.Admin.Referral.UpdateSettings)
			referral.GET("/affiliates", h.Admin.Referral.ListAffiliates)
			referral.GET("/commissions", h.Admin.Referral.ListCommissions)
			referral.POST("/affiliates/:user_id/approve", h.Admin.Referral.ApproveAffiliate)
			referral.POST("/affiliates/:user_id/reject", h.Admin.Referral.RejectAffiliate)
			referral.POST("/affiliates/:user_id/disable", h.Admin.Referral.DisableAffiliate)
			referral.POST("/affiliates/:user_id/restore", h.Admin.Referral.RestoreAffiliate)
			referral.POST("/affiliates/:user_id/settlement/freeze", h.Admin.Referral.FreezeSettlement)
			referral.POST("/affiliates/:user_id/settlement/restore", h.Admin.Referral.RestoreSettlement)
			referral.POST("/affiliates/:user_id/withdrawal/freeze", h.Admin.Referral.FreezeWithdrawal)
			referral.POST("/affiliates/:user_id/withdrawal/restore", h.Admin.Referral.RestoreWithdrawal)
			referral.POST("/settlements/run", h.Admin.Referral.RunSettlementBatch)
			referral.GET("/withdrawals", h.Admin.Referral.ListWithdrawals)
			referral.POST("/withdrawals/:id/approve", h.Admin.Referral.ApproveWithdrawal)
			referral.POST("/withdrawals/:id/reject", h.Admin.Referral.RejectWithdrawal)
			referral.POST("/withdrawals/:id/pay", h.Admin.Referral.MarkWithdrawalPaid)
			referral.POST("/upload", h.Admin.Referral.UploadAsset)
		}
	}
}
