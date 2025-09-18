#!/usr/bin/env python3
"""
Demo script showing CK Intelligence with database access
"""

import sys
sys.path.append('.')

from app import get_gemini_response

def demo_interactive():
    """Interactive demo of CK Intelligence with database access."""
    print("🤖 CK Intelligence Demo - Database Access Enabled")
    print("=" * 60)
    print("Ask me anything about your OrderAI system data!")
    print("Try questions like:")
    print("  • 'What's our current inventory status?'")
    print("  • 'Show me the latest batch information'")
    print("  • 'How many orders have we processed?'")
    print("  • 'What fish types do we have in stock?'")
    print("  • 'Give me an overview of the system'")
    print("\nType 'quit' to exit\n")

    while True:
        try:
            user_input = input("You: ").strip()
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye! CK Intelligence is always here to help.")
                break

            if user_input:
                print("🤖 CK Intelligence: ", end="")
                response = get_gemini_response(user_input)
                print(response)
                print()

        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print()

def demo_examples():
    """Show example interactions."""
    print("📋 CK Intelligence Example Responses")
    print("=" * 60)

    examples = [
        "Give me an overview of the system",
        "What's our current inventory status?",
        "Show me the latest batch information",
        "How many processing sessions do we have?",
        "What fish types are in our inventory?"
    ]

    for i, example in enumerate(examples, 1):
        print(f"\n{i}. User: {example}")
        print("   CK Intelligence:", end=" ")
        response = get_gemini_response(example)
        # Show first 300 characters to keep demo concise
        print(f"{response[:300]}{'...' if len(response) > 300 else ''}")

    print(f"\n🎉 Demo completed! Try the interactive mode with: python demo_ckintelligence.py interactive")

def main():
    """Main demo function."""
    if len(sys.argv) > 1 and sys.argv[1] == 'interactive':
        demo_interactive()
    else:
        demo_examples()

if __name__ == "__main__":
    main()
