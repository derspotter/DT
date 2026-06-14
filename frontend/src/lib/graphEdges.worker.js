// Web Worker wrapper around buildEdgeBuffers so the per-edge Bézier sampling
// runs off the main thread. Inputs arrive structured-cloned (left intact on the
// main thread so it can fall back to a synchronous build if the worker fails);
// the three result buffers are transferred back zero-copy.
import { buildEdgeBuffers } from './graphEdges.js'

self.onmessage = (event) => {
  const { id, params } = event.data
  const result = buildEdgeBuffers(params)
  self.postMessage(
    { id, result },
    [result.edgePositions.buffer, result.edgeColors.buffer, result.endpointPairs.buffer]
  )
}
