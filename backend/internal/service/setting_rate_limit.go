package service

import "context"

type RegisterRateLimitSettings struct {
	Enabled                 bool
	PerIPPerMinute          int
	PerEmailPerMinute       int
	PerEmailDomainPerMinute int
	PerInviteCodePerMinute  int
}

type ReferralLandingRateLimitSettings struct {
	Enabled                bool
	PerIPPerMinute         int
	PerInviteCodePerMinute int
}

func DefaultRegisterRateLimitSettings() RegisterRateLimitSettings {
	return RegisterRateLimitSettings{
		Enabled:                 true,
		PerIPPerMinute:          5,
		PerEmailPerMinute:       0,
		PerEmailDomainPerMinute: 0,
		PerInviteCodePerMinute:  0,
	}
}

func DefaultReferralLandingRateLimitSettings() ReferralLandingRateLimitSettings {
	return ReferralLandingRateLimitSettings{
		Enabled:                true,
		PerIPPerMinute:         60,
		PerInviteCodePerMinute: 0,
	}
}

func (s *SettingService) GetRegisterRateLimitSettings(_ context.Context) RegisterRateLimitSettings {
	settings := DefaultRegisterRateLimitSettings()
	if s == nil || s.cfg == nil {
		return settings
	}
	cfg := s.cfg.RateLimit.Register
	settings.Enabled = cfg.Enabled
	settings.PerIPPerMinute = cfg.PerIPPerMinute
	settings.PerEmailPerMinute = cfg.PerEmailPerMinute
	settings.PerEmailDomainPerMinute = cfg.PerEmailDomainPerMinute
	settings.PerInviteCodePerMinute = cfg.PerInviteCodePerMinute
	return settings
}

func (s *SettingService) GetReferralLandingRateLimitSettings(_ context.Context) ReferralLandingRateLimitSettings {
	settings := DefaultReferralLandingRateLimitSettings()
	if s == nil || s.cfg == nil {
		return settings
	}
	cfg := s.cfg.RateLimit.ReferralLanding
	settings.Enabled = cfg.Enabled
	settings.PerIPPerMinute = cfg.PerIPPerMinute
	settings.PerInviteCodePerMinute = cfg.PerInviteCodePerMinute
	return settings
}
