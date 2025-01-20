console.log('Environment Variables:');
console.log('SUPABASE_KEY:', process.env.SUPABASE_KEY);
console.log('SUPABASE_URL:', process.env.SUPABASE_URL);


const ENV = {
    SUPABASE_KEY: typeof process !== 'undefined' ? process.env.SUPABASE_KEY : 'development',
    SUPABASE_URL: typeof process !== 'undefined' ? process.env.SUPABASE_URL : 'http://localhost:3000',
    PORT: typeof process !== 'undefined' ? process.env.PORT : 3000
  };
  
  console.log('Environment Variables:');
  console.log('SUPABASE_KEY:', ENV.SUPABASE_KEY);
  console.log('SUPABASE_URL:', ENV.SUPABASE_URL);
  
  // Make ENV available globally in browser
  if (typeof window !== 'undefined') {
    window.ENV = ENV;
  }
  
  // Export for Node.js environments
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = ENV;
  }
  