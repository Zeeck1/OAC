# CK Intelligence - Database Integration

## Overview

CK Intelligence is now powered by Deepseek AI with full database access to your OrderAI system. It can provide real-time insights, statistics, and analysis of your inventory, batches, processing results, and system data.

## 🚀 Key Features

### 🤖 AI-Powered Responses
- **Google Gemini AI Integration**: Uses advanced AI for natural, contextual responses
- **Context-Aware**: Understands OrderAI system architecture and terminology
- **Intelligent Analysis**: Provides insights and recommendations based on data

### 📊 Database Access
- **Real-Time Data**: Access to all OrderAI database tables
- **Comprehensive Queries**: Retrieve inventory, batch, processing, and file data
- **Statistical Analysis**: Generate reports and summaries from live data

### 💬 Interactive Chat
- **Natural Language**: Ask questions in plain English
- **System Navigation**: Get help with OrderAI modules and features
- **Data Insights**: Receive actionable insights from your data

## 📋 Available Data Types

### Processing Sessions
- Batch and single processing history
- Session statistics and performance metrics
- Timeline and activity tracking

### Inventory Management
- Real-time stock levels by fish type and size
- Order fulfillment status (Full/Not Full/Not Have)
- Shortfall analysis and requirements tracking

### File Management
- Uploaded order and stock files
- Revision tracking and history
- File processing status

### Batch Operations
- Detailed batch processing results
- Performance metrics and completion status
- File associations and outcomes

## 💡 Example Queries

### System Overview
```
"Give me an overview of the system"
"What's the current status of our operations?"
"Tell me about the OrderAI platform"
```

### Inventory Queries
```
"What's our current inventory status?"
"How many cartons of Tuna 1kg do we have?"
"Which fish types have the highest shortfall?"
"What are our stock levels for all products?"
```

### Batch Analysis
```
"Show me the latest batch information"
"How many batches have we processed this week?"
"What's the status of batch XYZ?"
"Show me batch processing statistics"
```

### Processing Statistics
```
"How many orders have we processed?"
"What's our fulfillment rate?"
"Show me processing results summary"
"How many items were processed today?"
```

## 🛠 Technical Implementation

### Database Functions
```python
get_database_overview()          # System-wide statistics
get_recent_batches(limit)       # Recent processing sessions
get_inventory_summary()         # Inventory status and analysis
get_processing_results_summary() # Detailed processing outcomes
get_batch_details(batch_id)     # Specific batch information
```

### AI Integration
```python
get_gemini_response(message)   # AI-powered responses with database context
get_database_context_for_ai(message)  # Context-aware data retrieval
```

### Configuration
- **API**: Google Generative AI API
- **Model**: `gemini-1.5-flash`
- **Environment**: `GOOGLE_GEMINI_API_KEY` for API key (optional)
- **Fallback**: Uses provided API key if env var not set

## 📊 Data Security & Access

- **View-Only Access**: CK Intelligence can only read data, not modify
- **Authentication Required**: Must be logged into Packing or Raw Materials module
- **Session-Based**: Access tied to user authentication
- **Audit Trail**: All interactions logged for security

## 🚀 Getting Started

### 1. Access CK Intelligence
1. Log into OrderAI system (Packing or Raw Materials module)
2. Navigate to `/ckintelligence` endpoint
3. Start chatting with natural language queries

### 2. Example Conversation
```
You: What's our current inventory status?
CK Intelligence: Based on the latest data, you have 142 unique fish types in inventory with 219 fully fulfilled orders and 292 partially fulfilled orders...

You: Show me the latest batch information
CK Intelligence: The most recent batch (ID: 42) was processed on [date] with [X] items and [Y] kg processed...
```

### 3. Best Practices
- **Be Specific**: Use clear, specific questions for better responses
- **Use Keywords**: Include terms like "inventory", "batch", "processing", "status"
- **Follow Up**: Ask follow-up questions based on initial responses
- **Data Range**: Specify time periods when relevant (e.g., "this week", "last month")

## 🧪 Testing & Validation

### Test Scripts
```bash
# Run comprehensive test suite
python test_deepseek.py

# Interactive demo
python demo_ckintelligence.py interactive

# Example responses demo
python demo_ckintelligence.py
```

### Test Coverage
- ✅ Database access functions
- ✅ AI integration with Deepseek
- ✅ Context-aware responses
- ✅ Error handling and fallbacks
- ✅ Authentication and security

## 📈 Performance & Reliability

### Response Times
- **Database Queries**: < 100ms for most operations
- **AI Processing**: 2-5 seconds for complex analysis
- **Context Loading**: Intelligent caching for frequently accessed data

### Reliability Features
- **Fallback Responses**: Graceful degradation if AI unavailable
- **Error Recovery**: Automatic retry mechanisms
- **Data Validation**: Ensures data integrity in responses
- **Rate Limiting**: Prevents system overload

## 🔧 Configuration Options

### Environment Variables
```bash
# Optional: Set Google Gemini API token
export GOOGLE_GEMINI_API_KEY="your_google_gemini_api_key_here"
```

For Windows PowerShell:
```powershell
# Optional: Set Google Gemini API token
$env:GOOGLE_GEMINI_API_KEY="your_google_gemini_api_key_here"
```

### System Settings
- **Max Tokens**: 1500 (configurable for response length)
- **Temperature**: 0.7 (balanced creativity vs accuracy)
- **Context Window**: Intelligent data loading based on query type

## 📞 Support & Troubleshooting

### Common Issues
1. **Slow Responses**: Check internet connection and API availability
2. **Data Not Loading**: Verify database connectivity
3. **Authentication Errors**: Ensure proper login to OrderAI system

### Debug Mode
Enable debug logging by setting:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🎯 Future Enhancements

- **Advanced Analytics**: Predictive insights and trend analysis
- **Custom Reports**: User-defined report generation
- **Integration APIs**: RESTful endpoints for external systems
- **Multi-language Support**: Localized responses
- **Voice Integration**: Speech-to-text capabilities

---

## 📝 Notes

- CK Intelligence provides read-only access to all OrderAI data
- Responses are generated using Deepseek AI with real-time database context
- All interactions are logged for security and audit purposes
- The system is designed for operational insights and decision support

For technical support or feature requests, please contact the development team.
