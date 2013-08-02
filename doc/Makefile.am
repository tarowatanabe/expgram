if WITH_RST2HTML

index.html: ../README.rst
	$(RST2HTML) --syntax-highlight=short --stylesheet=html4css1.css,highlight.css,style.css ../README.rst > index.html

endif WITH_RST2HTML

css_files = \
highlight.css \
html4css1.css \
math.css \
style.css

EXTRA_DIST = \
$(css_files)
