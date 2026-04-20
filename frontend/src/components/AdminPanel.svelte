<script>
  export let inviteUsername = ''
  export let inviteEmail = ''
  export let inviteStatus = ''
  export let inviteError = false
  export let handleCreateInvitation = () => {}
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
</div>