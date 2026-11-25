package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadRegistryRPCSources(t *testing.T) {
	const validators = `
[[Validators]]
PrivateURL = "http://validator-1:8545"
[[Validators]]
PrivateURL = "http://validator-2:8545"
[[Validators]]
PrivateURL = "http://validator-1:8545"
`
	const registry = `
[Registry]
Enabled = true
ContractAddress = "0x0000000000000000000000000000000000000001"
`
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(validators+registry), 0600))
	require.Equal(t, []string{"http://validator-1:8545", "http://validator-2:8545"}, Load(path).RegistrySources())
}

func TestLoadRegistryRequiresSourceOnlyWhenEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`[Registry]
Enabled = true
ContractAddress = "0x0000000000000000000000000000000000000001"
`), 0600))
	require.PanicsWithValue(t, "registry: configure at least one Validators.PrivateURL", func() { Load(path) })
	require.NoError(t, os.WriteFile(path, []byte("[Registry]\nEnabled = false\n"), 0600))
	require.NotPanics(t, func() { Load(path) })
}
