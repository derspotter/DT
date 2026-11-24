// The browser download links the app creates (FE-16). Both set
// rel="noopener"; on a same-origin link without a target it changes nothing.

function clickLink(link) {
  link.rel = 'noopener'
  document.body.appendChild(link)
  link.click()
  link.remove()
}

// Saves a blob under `filename`; the object URL is revoked even when the
// click throws.
export function saveBlob(blob, filename) {
  const url = URL.createObjectURL(blob)
  try {
    const link = document.createElement('a')
    link.href = url
    link.download = filename
    clickLink(link)
  } finally {
    URL.revokeObjectURL(url)
  }
}

// Opens a download URL (a ticket URL whose answer is an attachment).
export function openDownloadUrl(url) {
  const link = document.createElement('a')
  link.href = url
  clickLink(link)
}
