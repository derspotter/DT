<script>
  import { columnsForTable } from '../lib/tableColumns'

  export let table = 'corpus'
  export let visibility = {}
  export let onChange = () => {}

  let open = false

  $: defs = columnsForTable(table)

  function toggle(key) {
    const next = { ...visibility, [key]: !visibility[key] }
    // Never let the user hide every column — an empty table has no affordance
    // to get back, and the picker itself would be the only thing left.
    if (!Object.values(next).some(Boolean)) return
    onChange(next)
  }

  let root = null

  function close(event) {
    if (!event.currentTarget.contains(event.relatedTarget)) open = false
  }

  // focusout alone misses clicks on non-focusable page areas, which leave the
  // menu open; close on any pointer press outside the picker as well.
  function closeOnOutsidePress(event) {
    if (open && root && !root.contains(event.target)) open = false
  }
</script>

<svelte:window on:pointerdown={closeOnOutsidePress} />

<div class="column-picker" bind:this={root} on:focusout={close}>
  <button class="secondary column-picker__toggle" type="button" on:click={() => (open = !open)} aria-expanded={open}>
    Columns ▾
  </button>
  {#if open}
    <div class="column-picker__menu" role="group" aria-label="Toggle columns">
      {#each defs as def (def.key)}
        <label class="column-picker__item">
          <input type="checkbox" checked={Boolean(visibility[def.key])} on:change={() => toggle(def.key)} />
          <span>{def.label}</span>
        </label>
      {/each}
    </div>
  {/if}
</div>
