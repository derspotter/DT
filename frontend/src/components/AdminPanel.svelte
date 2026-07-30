<script>
  export let inviteUsername = ''
  export let inviteEmail = ''
  export let inviteStatus = ''
  export let inviteError = false
  export let handleCreateInvitation = () => {}

  // External APIs & LLM (spec lines 13 & 14). Values are instance-wide and
  // override the corresponding environment variables when set; clearing a
  // field falls back to the environment.
  export let appSettings = null
  export let appSettingsStatus = ''
  export let appSettingsError = false
  export let appSettingsDraft = {}
  export let handleSaveAppSettings = () => {}
  export let showAdvancedSettings = false

  function secretHint(entry) {
    if (entry?.is_set) return 'Stored — leave blank to keep, type to replace'
    if (entry?.env_fallback) return 'Not stored; using the value from the environment'
    return 'Not set'
  }

  function fallbackHint(entry) {
    if (!entry?.env_fallback) return 'Not set'
    return `Environment default: ${entry.env_fallback}`
  }
</script>

<div class="admin-panel">
  <div class="card admin-panel__hero">
    <div class="workspace-panel-title">
      <h2 class="workspace-section-title">Admin</h2>
      <p>Keep admin-only actions out of the day-to-day corpus workflow.</p>
    </div>
  </div>

  <div class="card admin-panel__card">
    <div class="workspace-panel-header">
      <div class="workspace-panel-title">
        <h3 class="workspace-section-title">Invite User</h3>
        <p class="muted">Create invitations for new editors, viewers, or owners.</p>
      </div>
    </div>

    <form class="admin-invite-form" on:submit|preventDefault={handleCreateInvitation}>
      <label>
        <span class="muted small">Username</span>
        <input type="text" placeholder="username" bind:value={inviteUsername} required />
      </label>
      <label>
        <span class="muted small">Email</span>
        <input type="email" placeholder="user@example.com" bind:value={inviteEmail} required />
      </label>
      <button class="secondary" type="submit">Send invite</button>
      {#if inviteStatus}
        <p class={`${inviteError ? 'error' : 'muted'} admin-invite-form__status`}>{inviteStatus}</p>
      {/if}
    </form>
  </div>

  <div class="card admin-panel__card">
    <div class="workspace-panel-header">
      <div class="workspace-panel-title">
        <h3 class="workspace-section-title">External APIs &amp; LLM</h3>
        <p class="muted">Instance-wide. A blank field falls back to the server environment.</p>
      </div>
    </div>

    {#if !appSettings}
      <p class="muted">Loading settings…</p>
    {:else}
      <form class="admin-settings-form" on:submit|preventDefault={handleSaveAppSettings}>
        <h4 class="admin-settings-form__group">OpenAlex</h4>
        <label>
          <span class="muted small">API key</span>
          <input
            type="password"
            autocomplete="off"
            placeholder={appSettings.openalex_api_key?.is_set ? '•••• set' : 'Not set'}
            bind:value={appSettingsDraft.openalex_api_key}
          />
          <span class="muted small">{secretHint(appSettings.openalex_api_key)}</span>
        </label>
        <label>
          <span class="muted small">Requests per second</span>
          <input type="number" min="1" step="1" placeholder="30" bind:value={appSettingsDraft.openalex_rps} />
          <span class="muted small">{fallbackHint(appSettings.openalex_rps)}</span>
        </label>

        <h4 class="admin-settings-form__group">LLM</h4>
        <label>
          <span class="muted small">Provider</span>
          <select bind:value={appSettingsDraft.llm_provider}>
            <option value="">Use environment default</option>
            <option value="gemini">Gemini</option>
            <option value="openai">OpenAI-compatible</option>
          </select>
          <span class="muted small">{fallbackHint(appSettings.llm_provider)}</span>
        </label>
        <label>
          <span class="muted small">Base URL (OpenAI-compatible)</span>
          <input type="text" placeholder="https://api.openai.com/v1" bind:value={appSettingsDraft.openai_base_url} />
          <span class="muted small">{fallbackHint(appSettings.openai_base_url)}</span>
        </label>
        <label>
          <span class="muted small">OpenAI-compatible API key</span>
          <input
            type="password"
            autocomplete="off"
            placeholder={appSettings.openai_api_key?.is_set ? '•••• set' : 'Not set'}
            bind:value={appSettingsDraft.openai_api_key}
          />
          <span class="muted small">{secretHint(appSettings.openai_api_key)}</span>
        </label>
        <label>
          <span class="muted small">Gemini API key</span>
          <input
            type="password"
            autocomplete="off"
            placeholder={appSettings.gemini_api_key?.is_set ? '•••• set' : 'Not set'}
            bind:value={appSettingsDraft.gemini_api_key}
          />
          <span class="muted small">{secretHint(appSettings.gemini_api_key)}</span>
        </label>
        <label>
          <span class="muted small">Extraction model</span>
          <input type="text" placeholder="model name" bind:value={appSettingsDraft.extract_model} />
          <span class="muted small">{fallbackHint(appSettings.extract_model)}</span>
        </label>

        <button
          class="secondary admin-settings-form__toggle"
          type="button"
          on:click={() => (showAdvancedSettings = !showAdvancedSettings)}
        >{showAdvancedSettings ? 'Hide advanced' : 'Advanced'}</button>

        {#if showAdvancedSettings}
          <label>
            <span class="muted small">OpenAI-compatible model</span>
            <input type="text" placeholder="model name" bind:value={appSettingsDraft.openai_model} />
            <span class="muted small">{fallbackHint(appSettings.openai_model)}</span>
          </label>
          <label>
            <span class="muted small">Gemini model</span>
            <input type="text" placeholder="model name" bind:value={appSettingsDraft.gemini_model} />
            <span class="muted small">{fallbackHint(appSettings.gemini_model)}</span>
          </label>
        {/if}

        <button class="primary" type="submit">Save settings</button>
        {#if appSettingsStatus}
          <p class={appSettingsError ? 'error' : 'muted'}>{appSettingsStatus}</p>
        {/if}
      </form>
    {/if}
  </div>
</div>