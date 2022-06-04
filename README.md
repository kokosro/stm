# @kokosro/stm

simple Short Term Memory.

## Usage

```javascript

const Stm = require('@kokosro/stm')

const stm = new Stm(redisurl);

stm.connect().then(()=>{
  stm.set('somevar', { info })
   .then(()=>{
     stm.get('somevar').then(console.log);
   });
});
```


