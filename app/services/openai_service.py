"""
OpenAI Service Abstraction Layer

This module provides a clean abstraction for AI query processing,
making it easy to swap AI providers in the future.
"""

import os
import json
from typing import Dict, List, Optional, Any
from openai import OpenAI


class AIService:
    """
    Abstraction layer for AI services.
    Currently supports OpenAI GPT-4o/GPT-4.1.
    """
    
    def __init__(self, provider: str = 'openai'):
        """
        Initialize AI service with specified provider.
        
        Args:
            provider: AI provider name (currently only 'openai' supported)
        """
        self.provider = provider
        
        if provider == 'openai':
            api_key = os.environ.get('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set")
            
            # Initialize OpenAI client with minimal configuration
            self.client = OpenAI(
                api_key=api_key,
                timeout=30.0,  # 30 second timeout
                max_retries=2
            )
            self.model = os.environ.get('OPENAI_MODEL', 'gpt-4o')  # Default to GPT-4o
        else:
            raise ValueError(f"Unsupported AI provider: {provider}")
    
    def query(
        self,
        user_query: str,
        capabilities: Dict[str, Any],
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process a user query using AI to generate structured guidance.
        
        Args:
            user_query: The user's natural language question
            capabilities: The product capability map (JSON)
            context: Additional context (current page, user role, etc.)
        
        Returns:
            Structured response with guidance or alternatives
        """
        system_prompt = self._build_system_prompt(capabilities, context)
        user_prompt = self._build_user_prompt(user_query, context)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.3,  # Lower temperature for more deterministic responses
                max_tokens=1500
            )
            
            raw_response = response.choices[0].message.content
            parsed_response = json.loads(raw_response)
            
            return self._validate_response(parsed_response)
            
        except Exception as e:
            # Handle different error types with user-friendly messages
            error_message = str(e)
            user_friendly_message = "AI service error"
            
            if "insufficient_quota" in error_message or "quota" in error_message.lower():
                user_friendly_message = "OpenAI API quota exceeded. Please check your billing details at platform.openai.com or contact your administrator."
            elif "invalid_api_key" in error_message or "authentication" in error_message.lower():
                user_friendly_message = "Invalid OpenAI API key. Please check your configuration."
            elif "rate_limit" in error_message.lower():
                user_friendly_message = "OpenAI rate limit reached. Please try again in a moment."
            elif "timeout" in error_message.lower() or "timed out" in error_message.lower():
                user_friendly_message = "Request timed out. Please try again."
            elif "connection" in error_message.lower() or "network" in error_message.lower():
                user_friendly_message = "Network error. Please check your internet connection and try again."
            else:
                user_friendly_message = f"AI service error: {error_message}"
            
            return {
                "isSupported": False,
                "reason": user_friendly_message,
                "steps": [],
                "suggestedAlternatives": [],
                "confidence": 0.0,
                "error_type": "ai_service_error"
            }
    
    def _build_system_prompt(self, capabilities: Dict[str, Any], context: Dict[str, Any]) -> str:
        """
        Build the system prompt with capability map and instructions.
        """
        user_role = context.get('userRole', 'viewer')
        
        prompt = f"""You are an intelligent guide for a Data Visualization Platform.

Your role is to help users understand how to achieve specific tasks within the application.

CRITICAL RULES:
1. You MUST ONLY reference features, metrics, dimensions, and filters defined in the Capability Map below
2. NEVER suggest features that don't exist
3. If the user asks for something not supported, clearly say so and suggest the closest alternative
4. Always check role permissions - don't suggest features the user can't access
5. Be specific with step-by-step instructions using exact UI element names
6. Return responses in valid JSON format

USER CONTEXT:
- Role: {user_role}
- Current Page: {context.get('currentPage', 'unknown')}

PRODUCT CAPABILITY MAP:
{json.dumps(capabilities, indent=2)}

RESPONSE FORMAT (JSON):
{{
  "isSupported": boolean,  // true if the request can be fulfilled with existing features
  "reason": string,  // Brief explanation (required if isSupported is false)
  "steps": [  // Empty array if not supported
    {{
      "stepNumber": number,
      "action": string,  // Brief action title
      "details": string,  // Detailed instructions with exact UI element names
      "location": string  // Optional page/route if navigation needed
    }}
  ],
  "suggestedAlternatives": [  // Provide if request not supported or if helpful
    {{
      "title": string,
      "description": string,
      "steps": [string]  // Array of step descriptions
    }}
  ],
  "confidence": number  // 0.0 to 1.0 - how confident you are in this answer
}}

PERMISSION RULES:
- Financial metrics (revenue, cost, profit, profit_margin) require admin role
- If user role is not 'admin', do NOT suggest financial metrics
- Operational metrics are available to all roles

Remember: Be helpful but NEVER hallucinate features!"""
        
        return prompt
    
    def _build_user_prompt(self, user_query: str, context: Dict[str, Any]) -> str:
        """
        Build the user-specific prompt.
        """
        return f"""User question: "{user_query}"

Based on the capability map and the user's role, provide step-by-step guidance or explain why it's not possible.
Include alternatives if the exact request isn't supported.

Respond with valid JSON following the specified format."""
    
    def _validate_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and ensure response has required fields.
        """
        # Ensure required fields exist
        validated = {
            "isSupported": response.get("isSupported", False),
            "reason": response.get("reason", ""),
            "steps": response.get("steps", []),
            "suggestedAlternatives": response.get("suggestedAlternatives", []),
            "confidence": min(max(response.get("confidence", 0.5), 0.0), 1.0)  # Clamp to 0-1
        }
        
        # Ensure steps are properly formatted
        for i, step in enumerate(validated["steps"]):
            if "stepNumber" not in step:
                step["stepNumber"] = i + 1
        
        return validated


def get_ai_service(provider: str = 'openai') -> AIService:
    """
    Factory function to get an AI service instance.
    
    Args:
        provider: AI provider name
    
    Returns:
        AIService instance
    """
    return AIService(provider=provider)
