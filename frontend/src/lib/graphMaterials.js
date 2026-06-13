// Custom shader materials for the 3D graph. PointsMaterial cannot vary point
// size or opacity per vertex, and LineBasicMaterial cannot vary opacity per
// vertex, so both are replaced with thin ShaderMaterials that add `size`
// (points only) and `alpha` attributes while keeping the original look:
// screen-space point sizes, additive blending, no depth test for points.

export function createNodeMaterial(THREE, pixelRatio = 1) {
  return new THREE.ShaderMaterial({
    uniforms: {
      uPixelRatio: { value: pixelRatio },
      uFade: { value: 1 },
      // View distance at which a node renders at its base pixel size. Closer
      // than this it grows, farther it shrinks — so zooming into a cluster
      // makes nodes bigger instead of staying fixed dots that fly out of view.
      uRefDist: { value: 3200 },
    },
    vertexShader: `
      attribute float size;
      attribute float alpha;
      uniform float uPixelRatio;
      uniform float uRefDist;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        vColor = color;
        vAlpha = alpha;
        vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
        // Perspective size attenuation, clamped so distant nodes stay visible
        // (>=2px) and close ones don't balloon off-screen (<=48px).
        float atten = uRefDist / max(60.0, -mvPosition.z);
        gl_PointSize = clamp(size * atten, 2.0, 30.0) * uPixelRatio;
        gl_Position = projectionMatrix * mvPosition;
      }
    `,
    fragmentShader: `
      uniform float uFade;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        // Anti-aliased solid disc. Normal (not additive) blending so zoomed-in
        // nodes read as crisp distinct dots instead of piling into a blur.
        float d = length(gl_PointCoord - vec2(0.5));
        float edge = smoothstep(0.5, 0.42, d);
        if (edge <= 0.0) discard;
        gl_FragColor = vec4(vColor, vAlpha * uFade * edge);
      }
    `,
    vertexColors: true,
    transparent: true,
    depthTest: false,
    depthWrite: false,
  })
}

// Node renderer. GL_POINTS sprites are dropped intermittently by some GPU
// drivers (notably Linux/Mesa and macOS/ANGLE) during camera motion or when
// scaled up. Instead render each node as an instanced camera-facing quad sized
// in screen pixels — identical look (an anti-aliased disc that grows as you
// zoom in) without the point-sprite driver bugs.
export function createNodeQuadMaterial(THREE, pixelRatio = 1, width = 1, height = 1) {
  return new THREE.ShaderMaterial({
    uniforms: {
      uPixelRatio: { value: pixelRatio },
      uFade: { value: 1 },
      uRefDist: { value: 3200 },
      uViewport: { value: new THREE.Vector2(width, height) },
    },
    vertexShader: `
      attribute vec3 aPosition;
      attribute vec3 aColor;
      attribute float aSize;
      attribute float aAlpha;
      uniform float uPixelRatio;
      uniform float uRefDist;
      uniform vec2 uViewport;
      varying vec2 vCorner;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        vColor = aColor;
        vAlpha = aAlpha;
        vCorner = position.xy; // base quad corner in [-0.5, 0.5]
        vec4 mvPosition = modelViewMatrix * vec4(aPosition, 1.0);
        float depth = max(60.0, -mvPosition.z);
        // Grows toward the camera, clamped so it never shrinks to nothing nor
        // grows so large that dense clusters tile into a solid mass (which made
        // individual nodes impossible to pick out).
        float px = clamp(aSize * (uRefDist / depth), 2.0, 8.0) * uPixelRatio;
        vec4 clip = projectionMatrix * mvPosition;
        // Offset the quad corner by px screen pixels, converted into clip space.
        clip.xy += (position.xy * px / (uViewport * 0.5)) * clip.w;
        gl_Position = clip;
      }
    `,
    fragmentShader: `
      uniform float uFade;
      varying vec2 vCorner;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        float d = length(vCorner);
        // Outer anti-aliased edge of the disc.
        float alphaMask = smoothstep(0.5, 0.46, d);
        if (alphaMask <= 0.0) discard;
        // Dark outline ring near the rim so overlapping/adjacent nodes (even of
        // the same colour) stay visually separable; bright fill in the centre.
        float fill = smoothstep(0.44, 0.38, d);
        vec3 col = mix(vColor * 0.22, vColor, fill);
        gl_FragColor = vec4(col, vAlpha * uFade * alphaMask);
      }
    `,
    transparent: true,
    depthTest: false,
    depthWrite: false,
  })
}

export function createEdgeMaterial(THREE) {
  return new THREE.ShaderMaterial({
    uniforms: {
      uFade: { value: 1 },
      // Faded toward 0 as the camera moves in close, so the edge convergence at
      // a cluster's centre stops veiling its nodes when you zoom in to inspect.
      uViewDim: { value: 1 },
    },
    vertexShader: `
      attribute float alpha;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        vColor = color;
        vAlpha = alpha;
        gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
      }
    `,
    fragmentShader: `
      uniform float uFade;
      uniform float uViewDim;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        gl_FragColor = vec4(vColor, vAlpha * uFade * uViewDim);
      }
    `,
    vertexColors: true,
    transparent: true,
    depthWrite: false,
    // Normal (not additive) blending: additive made bundled trunks stack up to
    // a saturated white smear that lost all colour. With a curated, lower edge
    // count the curved trunks read clearly as coloured strands instead.
  })
}
