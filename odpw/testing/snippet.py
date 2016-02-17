templateLoader = jinja2.FileSystemLoader(searchpath="." )
    templateEnv = jinja2.Environment(loader=templateLoader )
    template = templateEnv.get_template('bokeh.jinja')
    with open('bokeh.html', "w") as f:
        print "Writing ",f,'from',t
        f.write(template.render(data, title="J&#252;rgen Umbrich's Homepage", **t['args']).encode('utf8'))