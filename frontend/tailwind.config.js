/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      fontFamily: {
        display: ['"JetBrains Mono"', "ui-monospace", "monospace"],
        sans: ['"IBM Plex Sans"', "system-ui", "sans-serif"],
        mono: ['"JetBrains Mono"', "ui-monospace", "monospace"],
      },
      colors: {
        ink:    "#0b0d10",
        panel:  "#13161b",
        line:   "#1f242c",
        muted:  "#6b7280",
        text:   "#e6e8eb",
        accent: "#ff5a1f",       // Databricks-orange-ish
        ok:     "#22c55e",
        warn:   "#f59e0b",
        bad:    "#ef4444",
      },
    },
  },
  plugins: [],
};
