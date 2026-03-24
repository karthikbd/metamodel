/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx,ts,tsx}'],
  theme: {
    extend: {
      colors: {
        surface: {
          DEFAULT: '#0f0f10',
          card:    '#18181b',
          border:  '#27272a',
          hover:   '#1f1f23',
        },
        accent: {
          DEFAULT: '#7c3aed',
          hover:   '#6d28d9',
          muted:   '#4c1d95',
          text:    '#a78bfa',
        },
      },
      fontFamily: {
        mono: ['JetBrains Mono', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
    },
  },
  plugins: [],
}
