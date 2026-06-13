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
        gl_PointSize = clamp(size * atten, 2.0, 48.0) * uPixelRatio;
        gl_Position = projectionMatrix * mvPosition;
      }
    `,
    fragmentShader: `
      uniform float uFade;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        vec2 offset = gl_PointCoord - vec2(0.5);
        if (dot(offset, offset) > 0.25) discard;
        gl_FragColor = vec4(vColor, vAlpha * uFade);
      }
    `,
    vertexColors: true,
    transparent: true,
    blending: THREE.AdditiveBlending,
    depthTest: false,
    depthWrite: false,
  })
}

export function createEdgeMaterial(THREE) {
  return new THREE.ShaderMaterial({
    uniforms: {
      uFade: { value: 1 },
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
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        gl_FragColor = vec4(vColor, vAlpha * uFade);
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
