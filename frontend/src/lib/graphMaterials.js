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
    },
    vertexShader: `
      attribute float size;
      attribute float alpha;
      uniform float uPixelRatio;
      varying vec3 vColor;
      varying float vAlpha;
      void main() {
        vColor = color;
        vAlpha = alpha;
        gl_PointSize = size * uPixelRatio;
        gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
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
  })
}
