console.log('Environment Variables:');

// Check if running on the server or browser
const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;

const ENV = {
  NEXT_PUBLIC_SUPABASE_KEY : isNode ? process.env.NEXT_PUBLIC_SUPABASE_KEY  : (typeof window !== 'undefined' ? window.NEXT_PUBLIC_SUPABASE_KEY  : 'development'),
  NEXT_PUBLIC_SUPABASE_URL: isNode ? process.env.NEXT_PUBLIC_SUPABASE_URL : (typeof window !== 'undefined' ? window.NEXT_PUBLIC_SUPABASE_URL : 'http://localhost:3000'),
  PORT: isNode ? process.env.PORT : 3000
};

console.log('NEXT_PUBLIC_SUPABASE_KEY :', ENV.NEXT_PUBLIC_SUPABASE_KEY );
console.log('NEXT_PUBLIC_SUPABASE_URL:', ENV.NEXT_PUBLIC_SUPABASE_URL);

// In the browser, you can manually assign window variables or inject them during the build
if (typeof window !== 'undefined') {
  window.ENV = ENV;
}

// Export for Node.js environments
if (isNode && typeof module !== 'undefined' && module.exports) {
  module.exports = ENV;
}
