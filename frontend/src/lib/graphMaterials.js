// Custom shader materials for the 3D graph. LineBasicMaterial cannot vary
// opacity per vertex, and GL_POINTS sprites are unreliable across drivers, so
// edges and nodes use thin ShaderMaterials with per-instance/vertex colour and
// alpha.

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
        // 60.0 is a near-camera floor on view depth so a node approaching/crossing
        // the camera plane doesn't blow up to an extreme size.
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
        // Outer anti-aliased edge of the disc. smoothstep requires edge0 < edge1
        // (reversed edges are undefined in GLSL), so use increasing edges and
        // invert.
        float alphaMask = 1.0 - smoothstep(0.46, 0.5, d);
        if (alphaMask <= 0.0) discard;
        // Dark outline ring near the rim so overlapping/adjacent nodes (even of
        // the same colour) stay visually separable; bright fill in the centre.
        float fill = 1.0 - smoothstep(0.38, 0.44, d);
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
      // Faded down (to a ~0.12 floor) as the camera moves in close, so the edge
      // convergence at a cluster's centre stops veiling its nodes when you zoom
      // in to inspect; full strength in the overview.
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
