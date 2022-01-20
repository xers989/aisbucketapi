const express = require('express');
const MongoClient = require('mongodb').MongoClient;
const dotenv = require('dotenv');
const router = express.Router();
const converter = require('json-2-csv');
const fs = require('fs');

dotenv.config();
const mongodb = process.env.MONGODB;
const databasename = process.env.DATABASE;
const connectionString = mongodb;
      

const client = new MongoClient(connectionString);


router.route('/ais1').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais1");

        let pipeline = [
            { '$match': {'$and': [{ mmsi: _mmsi},{aistime2unix: {"$gte":_frdate}},{aistime2unix:{"$lt":_frdate+ _days*10 * 3600 * 24 }}]}},
            {'$sort':{'aistime2unix':1}}
          ];

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        res.status(200).send(csv);

        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});

router.route('/ais2').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais1");

        let pipeline = [
            { '$match': {'$and': [{ mmsi: _mmsi},{aistime2unix: {"$gte":_frdate}},{aistime2unix:{"$lt":_frdate+ _days*10 * 3600 * 24 }}]}},
            {'$project':{'_id':0,mmsi:1,longitude:1,latitude:1,sog:1,cog:1,aistime2unix:1}},
            {'$sort':{'aistime2unix':1}}
          ];

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        res.status(200).send(csv);

        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }    
});


router.route('/ais10min1').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = [
            { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
            {'$project':{'_id':0,'mmsi':1,'ais_measures':1}},
            {'$unwind':'$ais_measures'},
            {'$sort':{'aistime2unix':1}}
          ];
          let allowdisk = { allowDiskUse: true }
        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline, allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        res.status(200).send(csv);

        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});
router.route('/ais10min2').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = [
            {'$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
            {'$project':{'_id':0,'mmsi':1,'aisgroupunix':1,'ais_measures.coordinates':1,'ais_measures.sog':1,'ais_measures.cog':1,'ais_measures.aistime2unix':1}},
            {'$unwind':'$ais_measures'},
            {'$sort':{'aistime2unix':1}}
          ];

          let allowdisk = { allowDiskUse: true }
        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline, allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        res.status(200).send(csv);

        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});

router.route('/ais10min1all').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = [
            { '$match': { mmsi: _mmsi } },
            {'$project':{'_id':0,'mmsi':1,'ais_measures':1}},
            {'$unwind':'$ais_measures'},
            {'$sort':{'aistime2unix':1}}
          ];
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        res.status(200).send("OK");

        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});
router.route('/ais10min2all').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = [
            {'$match': { mmsi: _mmsi } },
            {'$project':{'_id':0,'mmsi':1,'aisgroupunix':1,'ais_measures.coordinates':1,'ais_measures.sog':1,'ais_measures.cog':1,'ais_measures.aistime2unix':1}},
            {'$unwind':'$ais_measures'},
            {'$sort':{'aistime2unix':1}}
          ];
          let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline, allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        res.status(200).send("OK");

        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});



router.route('/aisallsample1hour').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais11hourgeo");

        let pipeline = [
            { '$match': { mmsi: _mmsi } },
            { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
            {
                '$bucketAuto': {
                  groupBy: '$aisgroupunix',
                  buckets: _sample,
                  output: {
                    mmsi: { '$first': '$mmsi' },
                    msgtype: { '$first': '$firstAISdata.msgtype' },
                    class: { '$first': '$firstAISdata.class' },
                    aistime2date: { '$first': '$firstAISdata.aistime2date' },
                    aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                    coordinates: { '$first': '$firstAISdata.coordinates' },
                    trueheading: { '$first': '$firstAISdata.trueheading' },
                    rot: { '$first': '$firstAISdata.rot' },
                    sog: { '$first': '$firstAISdata.cog' },
                    cog: { '$first': '$firstAISdata.cog' },
                    navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                    positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                    regapplication: { '$first': '$firstAISdata.regapplication' },
                    raimflag: { '$first': '$firstAISdata.raimflag' },
                    commstate: { '$first': '$firstAISdata.commstate' },
                    fixdevice: { '$first': '$firstAISdata.fixdevice' }
                  }
                }
              },
            {'$sort':{'aistime2unix':1}}
          ];
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});

router.route('/aisallsample30min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais130mingeo");

        let pipeline = [
            { '$match': { mmsi: _mmsi } },
            { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
            {
                '$bucketAuto': {
                  groupBy: '$aisgroupunix',
                  buckets: _sample,
                  output: {
                    mmsi: { '$first': '$mmsi' },
                    msgtype: { '$first': '$firstAISdata.msgtype' },
                    class: { '$first': '$firstAISdata.class' },
                    aistime2date: { '$first': '$firstAISdata.aistime2date' },
                    aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                    coordinates: { '$first': '$firstAISdata.coordinates' },
                    trueheading: { '$first': '$firstAISdata.trueheading' },
                    rot: { '$first': '$firstAISdata.rot' },
                    sog: { '$first': '$firstAISdata.cog' },
                    cog: { '$first': '$firstAISdata.cog' },
                    navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                    positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                    regapplication: { '$first': '$firstAISdata.regapplication' },
                    raimflag: { '$first': '$firstAISdata.raimflag' },
                    commstate: { '$first': '$firstAISdata.commstate' },
                    fixdevice: { '$first': '$firstAISdata.fixdevice' }
                  }
                }
              },
            {'$sort':{'aistime2unix':1}}
          ];
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});



router.route('/aisallsample15min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais115mingeo");

        let pipeline = [
            { '$match': { mmsi: _mmsi } },
            { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
            {
                '$bucketAuto': {
                  groupBy: '$aisgroupunix',
                  buckets: _sample,
                  output: {
                    mmsi: { '$first': '$mmsi' },
                    msgtype: { '$first': '$firstAISdata.msgtype' },
                    class: { '$first': '$firstAISdata.class' },
                    aistime2date: { '$first': '$firstAISdata.aistime2date' },
                    aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                    coordinates: { '$first': '$firstAISdata.coordinates' },
                    trueheading: { '$first': '$firstAISdata.trueheading' },
                    rot: { '$first': '$firstAISdata.rot' },
                    sog: { '$first': '$firstAISdata.cog' },
                    cog: { '$first': '$firstAISdata.cog' },
                    navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                    positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                    regapplication: { '$first': '$firstAISdata.regapplication' },
                    raimflag: { '$first': '$firstAISdata.raimflag' },
                    commstate: { '$first': '$firstAISdata.commstate' },
                    fixdevice: { '$first': '$firstAISdata.fixdevice' }
                  }
                }
              },
            {'$sort':{'aistime2unix':1}}
          ];
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});


router.route('/aisallsample10min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = [
            { '$match': { mmsi: _mmsi } },
            { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
            {
                '$bucketAuto': {
                  groupBy: '$aisgroupunix',
                  buckets: _sample,
                  output: {
                    mmsi: { '$first': '$mmsi' },
                    msgtype: { '$first': '$firstAISdata.msgtype' },
                    class: { '$first': '$firstAISdata.class' },
                    aistime2date: { '$first': '$firstAISdata.aistime2date' },
                    aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                    coordinates: { '$first': '$firstAISdata.coordinates' },
                    trueheading: { '$first': '$firstAISdata.trueheading' },
                    rot: { '$first': '$firstAISdata.rot' },
                    sog: { '$first': '$firstAISdata.cog' },
                    cog: { '$first': '$firstAISdata.cog' },
                    navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                    positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                    regapplication: { '$first': '$firstAISdata.regapplication' },
                    raimflag: { '$first': '$firstAISdata.raimflag' },
                    commstate: { '$first': '$firstAISdata.commstate' },
                    fixdevice: { '$first': '$firstAISdata.fixdevice' }
                  }
                }
              },
            {'$sort':{'aistime2unix':1}}
          ];
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});


router.route('/aissample10min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = "";
        if (_days >= 7)
        {
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
                {
                    '$bucketAuto': {
                      groupBy: '$aisgroupunix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        msgtype: { '$first': '$firstAISdata.msgtype' },
                        class: { '$first': '$firstAISdata.class' },
                        aistime2date: { '$first': '$firstAISdata.aistime2date' },
                        aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                        coordinates: { '$first': '$firstAISdata.coordinates' },
                        trueheading: { '$first': '$firstAISdata.trueheading' },
                        rot: { '$first': '$firstAISdata.rot' },
                        sog: { '$first': '$firstAISdata.cog' },
                        cog: { '$first': '$firstAISdata.cog' },
                        navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                        positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                        regapplication: { '$first': '$firstAISdata.regapplication' },
                        raimflag: { '$first': '$firstAISdata.raimflag' },
                        commstate: { '$first': '$firstAISdata.commstate' },
                        fixdevice: { '$first': '$firstAISdata.fixdevice' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];
        }
        else{
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'ais_measures':1}},
                {$unwind:"$ais_measures"},
                {
                    '$bucketAuto': {
                      groupBy: '$ais_measures.aistime2unix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        msgtype: { '$first': '$ais_measures.msgtype' },
                        class: { '$first': '$ais_measures.class' },
                        aistime2date: { '$first': '$ais_measures.aistime2date' },
                        aistime2unix: { '$first': '$ais_measures.aistime2unix' },
                        coordinates: { '$first': '$ais_measures.coordinates' },
                        trueheading: { '$first': '$ais_measures.trueheading' },
                        rot: { '$first': '$ais_measures.rot' },
                        sog: { '$first': '$ais_measures.cog' },
                        cog: { '$first': '$ais_measures.cog' },
                        navigationalstatus: { '$first': '$ais_measures.navigationalstatus' },
                        positionaccuracy: { '$first': '$ais_measures.positionaccuracy' },
                        regapplication: { '$first': '$ais_measures.regapplication' },
                        raimflag: { '$first': '$ais_measures.raimflag' },
                        commstate: { '$first': '$ais_measures.commstate' },
                        fixdevice: { '$first': '$ais_measures.fixdevice' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];

        }

        
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});


router.route('/aissample30min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais130mingeo");

        let pipeline = "";
        if (_days >= 35)
        {
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
                {
                    '$bucketAuto': {
                      groupBy: '$aisgroupunix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        msgtype: { '$first': '$firstAISdata.msgtype' },
                        class: { '$first': '$firstAISdata.class' },
                        aistime2date: { '$first': '$firstAISdata.aistime2date' },
                        aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                        coordinates: { '$first': '$firstAISdata.coordinates' },
                        trueheading: { '$first': '$firstAISdata.trueheading' },
                        rot: { '$first': '$firstAISdata.rot' },
                        sog: { '$first': '$firstAISdata.cog' },
                        cog: { '$first': '$firstAISdata.cog' },
                        navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                        positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                        regapplication: { '$first': '$firstAISdata.regapplication' },
                        raimflag: { '$first': '$firstAISdata.raimflag' },
                        commstate: { '$first': '$firstAISdata.commstate' },
                        fixdevice: { '$first': '$firstAISdata.fixdevice' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];
        }
        else{
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'ais_measures':1}},
                {$unwind:"$ais_measures"},
                {
                    '$bucketAuto': {
                      groupBy: '$ais_measures.aistime2unix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        msgtype: { '$first': '$ais_measures.msgtype' },
                        class: { '$first': '$ais_measures.class' },
                        aistime2date: { '$first': '$ais_measures.aistime2date' },
                        aistime2unix: { '$first': '$ais_measures.aistime2unix' },
                        coordinates: { '$first': '$ais_measures.coordinates' },
                        trueheading: { '$first': '$ais_measures.trueheading' },
                        rot: { '$first': '$ais_measures.rot' },
                        sog: { '$first': '$ais_measures.cog' },
                        cog: { '$first': '$ais_measures.cog' },
                        navigationalstatus: { '$first': '$ais_measures.navigationalstatus' },
                        positionaccuracy: { '$first': '$ais_measures.positionaccuracy' },
                        regapplication: { '$first': '$ais_measures.regapplication' },
                        raimflag: { '$first': '$ais_measures.raimflag' },
                        commstate: { '$first': '$ais_measures.commstate' },
                        fixdevice: { '$first': '$ais_measures.fixdevice' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];

        }

        
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});


router.route('/aissample1hour').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = "";
        if (_days >= 70)
        {
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                { '$project': { _id: 0, mmsi: 1, aisgroupunix: 1, firstAISdata: 1 } },
                {
                    '$bucketAuto': {
                      groupBy: '$aisgroupunix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        msgtype: { '$first': '$firstAISdata.msgtype' },
                        class: { '$first': '$firstAISdata.class' },
                        aistime2date: { '$first': '$firstAISdata.aistime2date' },
                        aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                        coordinates: { '$first': '$firstAISdata.coordinates' },
                        trueheading: { '$first': '$firstAISdata.trueheading' },
                        rot: { '$first': '$firstAISdata.rot' },
                        sog: { '$first': '$firstAISdata.cog' },
                        cog: { '$first': '$firstAISdata.cog' },
                        navigationalstatus: { '$first': '$firstAISdata.navigationalstatus' },
                        positionaccuracy: { '$first': '$firstAISdata.positionaccuracy' },
                        regapplication: { '$first': '$firstAISdata.regapplication' },
                        raimflag: { '$first': '$firstAISdata.raimflag' },
                        commstate: { '$first': '$firstAISdata.commstate' },
                        fixdevice: { '$first': '$firstAISdata.fixdevice' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];
        }
        else{
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'ais_measures':1}},
                {$unwind:"$ais_measures"},
                {
                    '$bucketAuto': {
                      groupBy: '$ais_measures.aistime2unix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        msgtype: { '$first': '$ais_measures.msgtype' },
                        class: { '$first': '$ais_measures.class' },
                        aistime2date: { '$first': '$ais_measures.aistime2date' },
                        aistime2unix: { '$first': '$ais_measures.aistime2unix' },
                        coordinates: { '$first': '$ais_measures.coordinates' },
                        trueheading: { '$first': '$ais_measures.trueheading' },
                        rot: { '$first': '$ais_measures.rot' },
                        sog: { '$first': '$ais_measures.cog' },
                        cog: { '$first': '$ais_measures.cog' },
                        navigationalstatus: { '$first': '$ais_measures.navigationalstatus' },
                        positionaccuracy: { '$first': '$ais_measures.positionaccuracy' },
                        regapplication: { '$first': '$ais_measures.regapplication' },
                        raimflag: { '$first': '$ais_measures.raimflag' },
                        commstate: { '$first': '$ais_measures.commstate' },
                        fixdevice: { '$first': '$ais_measures.fixdevice' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];

        }

        
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});




router.route('/ais2sample10min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = "";
        if (_days >= 7)
        {
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'aisgroupunix':1,'firstAISdata.coordinates':1,'firstAISdata.sog':1,'firstAISdata.cog':1,'firstAISdata.aistime2unix':1}},
                {
                    '$bucketAuto': {
                      groupBy: '$aisgroupunix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                        coordinates: { '$first': '$firstAISdata.coordinates' },
                        sog: { '$first': '$firstAISdata.cog' },
                        cog: { '$first': '$firstAISdata.cog' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];
        }
        else{
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'ais_measures.coordinates':1,'ais_measures.sog':1,'ais_measures.cog':1,'ais_measures.aistime2unix':1}},
                {$unwind:"$ais_measures"},
                {
                    '$bucketAuto': {
                      groupBy: '$ais_measures.aistime2unix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        aistime2unix: { '$first': '$ais_measures.aistime2unix' },
                        coordinates: { '$first': '$ais_measures.coordinates' },
                        sog: { '$first': '$ais_measures.cog' },
                        cog: { '$first': '$ais_measures.cog' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];

        }

        
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});


router.route('/ais2sample30min').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais130mingeo");

        let pipeline = "";
        if (_days >= 35)
        {
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'aisgroupunix':1,'firstAISdata.coordinates':1,'firstAISdata.sog':1,'firstAISdata.cog':1,'firstAISdata.aistime2unix':1}},
                {
                    '$bucketAuto': {
                      groupBy: '$aisgroupunix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                        coordinates: { '$first': '$firstAISdata.coordinates' },
                        sog: { '$first': '$firstAISdata.cog' },
                        cog: { '$first': '$firstAISdata.cog' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];
        }
        else{
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'ais_measures.coordinates':1,'ais_measures.sog':1,'ais_measures.cog':1,'ais_measures.aistime2unix':1}},
                {$unwind:"$ais_measures"},
                {
                    '$bucketAuto': {
                      groupBy: '$ais_measures.aistime2unix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        aistime2unix: { '$first': '$ais_measures.aistime2unix' },
                        coordinates: { '$first': '$ais_measures.coordinates' },
                        sog: { '$first': '$ais_measures.cog' },
                        cog: { '$first': '$ais_measures.cog' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];

        }

        
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});


router.route('/ais2sample1hour').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110mingeo");

        let pipeline = "";
        if (_days >= 70)
        {
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'aisgroupunix':1,'firstAISdata.coordinates':1,'firstAISdata.sog':1,'firstAISdata.cog':1,'firstAISdata.aistime2unix':1}},
                {
                    '$bucketAuto': {
                      groupBy: '$aisgroupunix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        aistime2unix: { '$first': '$firstAISdata.aistime2unix' },
                        coordinates: { '$first': '$firstAISdata.coordinates' },
                        sog: { '$first': '$firstAISdata.cog' },
                        cog: { '$first': '$firstAISdata.cog' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];
        }
        else{
            pipeline = [
                { '$match': { '$and': [ { mmsi: _mmsi }, { aisgroupunix: { '$gte': _frdate } }, { aisgroupunix: { '$lt': _frdate + _days * 3600 * 24 } }] } },
                {'$project':{'_id':0,'mmsi':1,'ais_measures.coordinates':1,'ais_measures.sog':1,'ais_measures.cog':1,'ais_measures.aistime2unix':1}},
                {$unwind:"$ais_measures"},
                {
                    '$bucketAuto': {
                      groupBy: '$ais_measures.aistime2unix',
                      buckets: _sample,
                      output: {
                        mmsi: { '$first': '$mmsi' },
                        aistime2unix: { '$first': '$ais_measures.aistime2unix' },
                        coordinates: { '$first': '$ais_measures.coordinates' },
                        sog: { '$first': '$ais_measures.cog' },
                        cog: { '$first': '$ais_measures.cog' }
                      }
                    }
                  },
                  { '$project': { _id: 0} },
                {'$sort':{'aistime2unix':1}}
              ];

        }

        
        
        let allowdisk = { allowDiskUse: true }

        console.log("pipeline:"+JSON.stringify(pipeline));

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});



router.route('/ais1sample').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais1");

        let pipeline = [
            {
                '$match': {
                  '$and': [
                    { mmsi: _mmsi },
                    { aistime2unix: { '$gte': _frdate } },
                    { aistime2unix: { '$lt': _frdate +  _days * 3600 * 24} }
                  ]
                }
              },
              {
                '$bucketAuto': {
                  groupBy: '$aistime2unix',
                  buckets: _sample,
                  output: {
                    mmsi: { '$first': '$mmsi' },
                    msgtype: { '$first': '$msgtype' },
                    class: { '$first': '$class' },
                    aistime2date: { '$first': '$aistime2date' },
                    aistime2unix: { '$first': '$aistime2unix' },
                    longitude: { '$first': '$longitude' },
                    latitude: { '$first': '$latitude' },
                    trueheading: { '$first': '$trueheading' },
                    rot: { '$first': '$rot' },
                    sog: { '$first': '$cog' },
                    cog: { '$first': '$cog' },
                    navigationalstatus: { '$first': '$navigationalstatus' },
                    positionaccuracy: { '$first': '$positionaccuracy' },
                    regapplication: { '$first': '$regapplication' },
                    raimflag: { '$first': '$raimflag' },
                    commstate: { '$first': '$commstate' },
                    fixdevice: { '$first': '$fixdevice' }
                  }
                }
              },
              { '$project': { _id: 0} },
            {'$sort':{'aistime2unix':1}}
          ];

        console.log("pipeline:"+JSON.stringify(pipeline));

        let allowdisk = { allowDiskUse: true }

        const cursor = await ais1aggregate.aggregate(pipeline, allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(results);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send("OK");

        res.status(200).json(results);
        //res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }   
});

router.route('/ais2sample').get( async(req, res, next) => {
    try{
        let _mmsi = parseInt(req.query.mmsi);
        let _sample = parseInt(req.query.sample);
        let _frdate = parseInt(req.query.from);
        let _days = parseInt(req.query.days);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais1");

        let pipeline = [
            { '$match': {'$and': [{ mmsi: _mmsi},{aistime2unix: {"$gte":_frdate}},{aistime2unix:{"$lt":_frdate+ _days*10 * 3600 * 24 }}]}},
            {'$project':{'_id':0,mmsi:1,longitude:1,latitude:1,sog:1,cog:1,aistime2unix:1}},
            {
                '$bucketAuto': {
                  groupBy: '$aistime2unix',
                  buckets: _sample,
                  output: {
                    mmsi: { '$first': '$mmsi' },
                    aistime2unix: { '$first': '$aistime2unix' },
                    longitude: { '$first': '$longitude' },
                    latitude: { '$first': '$latitude' },
                    sog: { '$first': '$cog' },
                    cog: { '$first': '$cog' }
                  }
                }
              },
              { '$project': { _id: 0} },
            {'$sort':{'aistime2unix':1}}
          ];

        console.log("pipeline:"+JSON.stringify(pipeline));

        let allowdisk = { allowDiskUse: true }

        const cursor = await ais1aggregate.aggregate(pipeline,allowdisk);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send(csv);

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }    
});


router.route('/aisgeo').get( async(req, res, next) => {
    try{
        let _long = parseFloat(req.query.long);
        let _lat = parseFloat(req.query.lat);
        let _distance = parseInt(req.query.distance);

        await client.connect();
        const database = client.db(databasename);
        const ais1aggregate = database.collection("ais110minsgeo");

        let pipeline = [
            { '$geoNear': { near: { type: 'point', coordinates: [_long,_lat] }, distanceField: 'dist.calculated',maxDistance:_distance,  includeLocs: 'dist.location', spherical:false } },
            { '$project': { _id: 0, ais_measures: 0,firstaistime2date:0,firstaistime2unix:0,lastaistime2date:0,lastaistime2unix:0, rowCount:0 } }
          ];

        console.log("pipeline:"+JSON.stringify(pipeline));


        const cursor = await ais1aggregate.aggregate(pipeline);
        
        const results = await cursor.toArray();

        let outcomes = '';
        if (results.length > 0) {
            results.forEach((result, i) => {
                outcomes += JSON.stringify(result);
                //console.log(result);
            });
        } else {
            console.log('No Data');
        }

        //const csv = await converter.json2csvAsync(results);
        //fs.writeFileSync('outcome.csv', csv);

        //console.log("Outcomes : "+outcomes);
        //res.status(200).send(csv);

        res.status(200).json(results);

    } catch(e)
    {
        console.log("Error");
        console.error(e);
        res.status(404).json({});

    }
    finally{
        await client.close();
    }    
});

module.exports = router;