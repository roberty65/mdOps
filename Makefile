
subdirs = statShare/src \
	  statAgentClient/src \
	  statAgentClient/test \
	  statAgentServer/src \
	  statAgentSystem/src \
	  statStorageServer/src \
	  statWebServer/src

targets = all clean distclean

$(targets):
	@for dir in $(subdirs); do if make -C $$dir $@; then : else exit; fi; done 
