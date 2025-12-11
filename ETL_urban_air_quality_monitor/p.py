import random

option1 = "Group 1: ea, ce | Group 2: cs, ct"
option2 = "Group 1: ea, ct | Group 2: cs, ce"

# Counters
count_option1 = 0
count_option2 = 0

# Simulate 100 selections
for _ in range(100):
    selected = random.choice([option1, option2])
    if selected == option1:
        count_option1 += 1
    else:
        count_option2 += 1

# Display counts
print(f"Option 1 selected: {count_option1} times")
print(f"Option 2 selected: {count_option2} times")

# Determine winner
if count_option1 > count_option2:
    print("Winner: Option 1 ✅")
elif count_option2 > count_option1:
    print("Winner: Option 2 ✅")
else:
    print("Tie! Both options selected equally 🔄")
