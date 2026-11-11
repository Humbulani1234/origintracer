/** @type {import('next').NextConfig} */
const isStaticDemoBuild = process.env.STATIC_DEMO_BUILD === "true";
const basePath = process.env.NEXT_BASE_PATH || "";

const nextConfig = {
  ...(basePath ? { basePath } : {}),
  env: {
    NEXT_PUBLIC_BASE_PATH: basePath,
  },
  ...(isStaticDemoBuild && {
    output: "export",
    distDir: "out-demo",
  }),
  devIndicators: false,
  experimental: {
    serverActions: {
      bodySizeLimit: "10mb",
    },
  },
};

export default nextConfig;
