console.log('Environment Variables:');

// Check if running on the server or browser
const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;

const ENV = {
  SUPABASE_KEY: isNode ? process.env.SUPABASE_KEY : (typeof window !== 'undefined' ? window.SUPABASE_KEY : 'development'),
  SUPABASE_URL: isNode ? process.env.SUPABASE_URL : (typeof window !== 'undefined' ? window.SUPABASE_URL : 'http://localhost:3000'),
  PORT: isNode ? process.env.PORT : 3000
};

console.log('SUPABASE_KEY:', ENV.SUPABASE_KEY);
console.log('SUPABASE_URL:', ENV.SUPABASE_URL);

// In the browser, you can manually assign window variables or inject them during the build
if (typeof window !== 'undefined') {
  window.ENV = ENV;
}

// Export for Node.js environments
if (isNode && typeof module !== 'undefined' && module.exports) {
  module.exports = ENV;
}
