#!/bin/bash

# Step 1: Get a real trace ID from the REPL
TRACE_ID=$(printf "SHOW events LIMIT 10\nexit\n" | python3 repl.py | \
  grep -oE '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)

echo "Captured trace ID: $TRACE_ID"

if [ -z "$TRACE_ID" ]; then
  echo "ERROR: Could not extract trace ID. Aborting."
  exit 1
fi

# Step 2: Generate the tape file — use single quotes inside Type to avoid escape issues
cat > /tmp/generated_demo.tape << EOF
Output stacktracer_demo.gif
Set Theme "Catppuccin Mocha"
Set FontSize 30
Set Width 2500
Set Height 2500
Set Padding 10
Set WindowBar Colorful

Type "python3 repl.py"
Sleep 5s
Enter
Sleep 5s

Type 'SHOW graph where system="http"'
Sleep 5s
Enter
Sleep 13s

Type "\\stitch $TRACE_ID"
Sleep 5s
Enter
Sleep 8s

Type 'SHOW nodes where system="http"'
Sleep 5s
Enter
Sleep 7s

Type 'SHOW edges where system="worker"'
Sleep 5s
Enter
Sleep 7s

Type "SHOW events LIMIT 5"
Sleep 5s
Enter
Sleep 7s

Type 'BLAME where system="http"'
Sleep 5s
Enter
Sleep 7s

Type "CAUSAL"
Sleep 5s
Enter
Sleep 12s

Type "exit"
Enter
Sleep 5s
EOF

# Step 3: Run VHS with the generated tape
vhs /tmp/generated_demo.tape