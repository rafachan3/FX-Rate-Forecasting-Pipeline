/**
 * Rate limiting test script
 * Run this in browser console or Node.js to test rate limiting
 * 
 * Usage in browser console:
 * 1. Open DevTools (F12)
 * 2. Go to Console tab
 * 3. Copy and paste this entire file
 * 4. Run: testRateLimit()
 */

async function testRateLimit() {
  const baseUrl = window.location.origin; // Use current domain
  const endpoint = `${baseUrl}/api/subscribe`;
  
  console.log('Testing rate limiting...');
  console.log(`Endpoint: ${endpoint}`);
  console.log('Making 6 requests (5th should succeed, 6th should return 429)...\n');
  
  const results = [];
  
  for (let i = 1; i <= 6; i++) {
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          email: `test${i}@example.com`,
          pairs: ['USD_CAD'],
          frequency: 'DAILY',
        }),
      });
      
      const data = await response.json();
      const status = response.status;
      const headers = {
        'X-RateLimit-Limit': response.headers.get('X-RateLimit-Limit'),
        'X-RateLimit-Remaining': response.headers.get('X-RateLimit-Remaining'),
        'X-RateLimit-Reset': response.headers.get('X-RateLimit-Reset'),
        'Retry-After': response.headers.get('Retry-After'),
      };
      
      results.push({
        request: i,
        status,
        allowed: status !== 429,
        data,
        headers,
      });
      
      console.log(`Request ${i}:`, {
        status,
        allowed: status !== 429,
        message: data.message || data.error,
        remaining: headers['X-RateLimit-Remaining'],
      });
      
      // Small delay between requests
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`Request ${i} failed:`, error);
      results.push({
        request: i,
        error: error.message,
      });
    }
  }
  
  console.log('\n=== Summary ===');
  const rateLimited = results.filter(r => r.status === 429);
  const successful = results.filter(r => r.status !== 429 && !r.error);
  
  console.log(`Successful requests: ${successful.length}`);
  console.log(`Rate limited requests: ${rateLimited.length}`);
  
  if (rateLimited.length > 0) {
    console.log('\nRate limit working correctly!');
    console.log('Rate limited request details:', rateLimited[0]);
  } else {
    console.log('\nWarning: No requests were rate limited. Check if limits are configured correctly.');
  }
  
  return results;
}

// Test unsubscribe rate limit
async function testUnsubscribeRateLimit() {
  const baseUrl = window.location.origin;
  const endpoint = `${baseUrl}/api/subscribe`;
  
  console.log('Testing unsubscribe rate limiting...');
  console.log(`Endpoint: ${endpoint}`);
  console.log('Making 11 DELETE requests (10th should succeed, 11th should return 429)...\n');
  
  const results = [];
  
  for (let i = 1; i <= 11; i++) {
    try {
      const response = await fetch(endpoint, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          email: `test${i}@example.com`,
        }),
      });
      
      const data = await response.json();
      const status = response.status;
      const headers = {
        'X-RateLimit-Limit': response.headers.get('X-RateLimit-Limit'),
        'X-RateLimit-Remaining': response.headers.get('X-RateLimit-Remaining'),
        'Retry-After': response.headers.get('Retry-After'),
      };
      
      results.push({
        request: i,
        status,
        allowed: status !== 429,
        data,
        headers,
      });
      
      console.log(`Request ${i}:`, {
        status,
        allowed: status !== 429,
        message: data.message || data.error,
        remaining: headers['X-RateLimit-Remaining'],
      });
      
      await new Promise(resolve => setTimeout(resolve, 100));
    } catch (error) {
      console.error(`Request ${i} failed:`, error);
    }
  }
  
  return results;
}

// Export for use
if (typeof window !== 'undefined') {
  window.testRateLimit = testRateLimit;
  window.testUnsubscribeRateLimit = testUnsubscribeRateLimit;
  console.log('Rate limit test functions loaded!');
  console.log('Run: testRateLimit() or testUnsubscribeRateLimit()');
}
