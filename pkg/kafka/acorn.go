package kafka

import (
	auacornapi "github.com/StephanHCB/go-autumn-acorn-registry/api"
	auzerolog "github.com/StephanHCB/go-autumn-logging-zerolog"
	"github.com/StephanHCB/go-backend-service-common/acorns/repository"
	"golang.org/x/net/context"
)

// --- implementing Acorn ---

func New() auacornapi.Acorn {
	return &Config{}
}

func (c *Config) AcornName() string {
	return repository.VaultAcornName
}

func (c *Config) AssembleAcorn(_ auacornapi.AcornRegistry) error {
	return nil
}

func (c *Config) SetupAcorn(registry auacornapi.AcornRegistry) error {
	if err := registry.SetupAfter(registry.GetAcornByName(repository.ConfigurationAcornName)); err != nil {
		return err
	}

	ctx := auzerolog.AddLoggerToCtx(context.Background())

	if err := c.Validate(ctx); err != nil {
		return err
	}
	c.Obtain(ctx)

	return nil
}

func (c *Config) TeardownAcorn(_ auacornapi.AcornRegistry) error {
	return nil
}
