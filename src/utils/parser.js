module.exports = {

    createObjKeys: (data) => {
        const obj = {}
        data[0].map(item => obj[item] = '')
        return obj
        // console.log(obj)
    },
    fillObj: (data) => {
        const result = [];
        let counter = 0;
        const keys = data.shift()
        const obj ={}
        for (let index = 0; index < keys.length; index++) {
            obj[keys[index]] = data[counter][index] 
        }
        return obj
    }

}