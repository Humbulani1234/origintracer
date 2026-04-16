# --- Configuration ---
Output stacktracer_demo.gif
Set Theme "Catppuccin Mocha" # Or "Dracula", "Monokai"
Set FontSize 30
Set Width 2500
Set Height 2500
Set Padding 10
Set WindowBar Colorful # Adds a sleek Mac-style top bar

# --- The Interaction ---

# 1. Start the tool
Type "python3 repl.py"
Sleep 5s
Enter
Sleep 5s

# 2. Get the Graph
Type "SHOW graph where system="http""
Sleep 5s
Enter
Sleep 13s # Give users time to look at the result

Type "SHOW nodes where system="http""
Sleep 5s
Enter
Sleep 7s

Type "SHOW edges where system="worker""
Sleep 5s
Enter
Sleep 7s

# 3. Show Events
Type "SHOW events LIMIT 5"
Sleep 5s
Enter
Sleep 7s

Type "BLAME where system="http""
Sleep 5s
Enter
Sleep 7s

#\stitch 4a509c84-6c9d-4d61-90ff-4a9518324203
#Sleep 3s
#Enter
#Sleep 5s

# 3. Show Events
Type "CAUSAL"
Sleep 5s
Enter
Sleep 12s

# 5. Clean exit
Type "exit"
Enter
Sleep 5s
