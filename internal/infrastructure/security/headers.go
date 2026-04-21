package security

import "github.com/gin-gonic/gin"

// SecurityHeadersMiddleware sets HTTP security headers to mitigate common vulnerabilities.
func SecurityHeadersMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Prevent MIME-type sniffing (e.g. serving a .txt as JS)
		c.Header("X-Content-Type-Options", "nosniff")

		// Block page from being framed (clickjacking protection)
		c.Header("X-Frame-Options", "DENY")

		// Enable browser XSS filter (legacy browsers)
		c.Header("X-XSS-Protection", "1; mode=block")

		// Force HTTPS for 1 year; include subdomains
		c.Header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

		// Restrict what the browser is allowed to send as Referer
		c.Header("Referrer-Policy", "strict-origin-when-cross-origin")

		// Disable browser features that aren't needed by the API
		c.Header("Permissions-Policy", "geolocation=(), microphone=(), camera=()")

		// Tight CSP — API responses are JSON only, no HTML rendering needed
		c.Header("Content-Security-Policy", "default-src 'none'")

		// Prevent caching of sensitive API responses
		c.Header("Cache-Control", "no-store")

		c.Next()
	}
}
