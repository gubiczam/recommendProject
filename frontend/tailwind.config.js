export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        ink: '#132024',
        ember: '#f85a3e',
        mist: '#f3f4ef',
        pine: '#1b4332',
      },
      boxShadow: {
        card: '0 12px 40px -16px rgba(19, 32, 36, 0.3)',
      },
    },
  },
  plugins: [],
}
