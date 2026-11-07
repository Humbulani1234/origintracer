/** @type {import('next').NextConfig} */
const isStaticDemoBuild = process.env.STATIC_DEMO_BUILD === "true";

const nextConfig = {
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
