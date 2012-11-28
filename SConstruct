import os

env = Environment()

def init_action(target, source, env):
	Mkdir('tutu')
	# Whatever it takes to build
	return None

def symlink(target, source, env):
    os.symlink(os.path.abspath(str(source[0])), os.path.abspath(str(target[0])))

#symlink_builder = Builder(action = "ln -s ${SOURCE.file} ${TARGET.file}", chdir = True)
symlink_builder = Builder(action = symlink, chdir = True)

env.Append(BUILDERS = {"Symlink" : symlink_builder})

mylib_link = env.Symlink("toto", "qserv-env.sh")

env.Alias('symlink', mylib_link)


#if Execute(action=init_action):
#        # A problem occurred while making the temp directory.
#        Exit(1)

qserv_init_bld = env.Builder(action=init_action)

env.Append(BUILDERS = {'Qserv_init' : qserv_init_bld})

env.Qserv_init([],[],[])
#Execute(Mkdir('tutu'))

#qserv_init_alias = env.Alias('qserv_inYit', env.Qserv_init())
#env.Alias('install', [qserv_init_alias])
