function DropDown(el) {
    
    this.dd = el;
    this.placeholder = this.dd.find('span.label');
    this.input = this.dd.find('input'); 
    this.opts = this.dd.find('.dd ul> li');
    this.dddiv = this.dd.find('.dd');
    this.input = this.dd.find('.dd input');
    this.val = '';
    this.index = -1;
    console.log("INIT Events")
    this.initEvents();
}

DropDown.prototype = {
    initEvents: function () {
        var obj = this;
        console.log("INIT Events"+obj.dd)
        
        //add search form
        obj.dddiv.hide()
        obj.dd.on('click', function (event) {
            obj.dddiv.toggle(); return false;
        });
        
        obj.dddiv.on('click', function(e){
        	e.stopPropagation();
        })
        obj.input.keyup(function(){
            var valThis = obj.input.val().toLowerCase();
            obj.opts.each(function(){
                var text = $(this).text().toLowerCase();
                if(text.indexOf(valThis) > -1)
                    $(this).show()
                else
                    $(this).hide();         
            });
        });
        
        obj.opts.on('click', function () {
            var opt = $(this);
            obj.val = opt.text();
            obj.index = opt.index();
            obj.placeholder.text(obj.val);
            obj.placeholder.trigger("change")
            obj.dddiv.toggle(); return false;
        });
        
        
    },
    getValue: function () {
        return this.val;
    },
    getIndex: function () {
        return this.index;
    }
}


function Card(el){
    this.div = el;
    this.val = '';
    this.index = -1;
    this.initEvents();
}
Card.prototype = {
    initEvents: function () {
        var obj = this;


    },
    getValue: function () {
        return this.val;
    },
    getIndex: function () {
        return this.index;
    }
}
