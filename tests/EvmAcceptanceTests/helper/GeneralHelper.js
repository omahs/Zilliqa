const axios = require('axios')

var general_helper = {
    callEthMethod: async function (method, id, params, callback) {
        const data = {
            id: id,
            jsonrpc: "2.0",
            method: method,
            params: params
        }

        const host = hre.getNetworkUrl()

        // ASYNC
        if (typeof callback === 'function') {
            await axios.post(host, data).then(response => {
                if (response.status === 200) {
                    callback(response.data, response.status);
                } else {
                    throw new Error('Can\'t connect to ' + host + "\n Send: " + JSON.stringify(data, null, 2));
                }
            })
            // SYNC
        } else {
            const response = await axios.post(host, data)

            if (response.status !== 200) {
                throw new Error('Can\'t connect to ' + host + "\n Send: " + JSON.stringify(data, null, 2));
            }

            return response.data
        }
    }
}

module.exports = general_helper