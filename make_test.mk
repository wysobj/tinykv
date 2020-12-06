PACKAGE_LIST:=go list ./...
PACKAGES:=$$($(PACKAGE_LIST))

foo:
	@echo "$(PACKAGES)"

