num_commands = 100000
command_file = open('commands.txt', 'w')
for i in range(0, num_commands):
    command_file.write('SET a ' + str(i) + '\n')
