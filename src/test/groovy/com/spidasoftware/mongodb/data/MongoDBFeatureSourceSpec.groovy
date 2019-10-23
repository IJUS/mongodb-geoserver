package com.spidasoftware.mongodb.data

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.MongoClient
import com.mongodb.ServerAddress
import com.mongodb.util.JSON
import com.spidasoftware.mongodb.feature.collection.MongoDBFeatureCollection
import org.geotools.data.Query
import org.geotools.feature.FeatureCollection
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.feature.NameImpl
import org.geotools.filter.text.cql2.CQL
import org.geotools.util.logging.Logging
import org.opengis.feature.type.FeatureType
import spock.lang.Shared
import spock.lang.Specification

import java.util.logging.Logger

class MongoDBFeatureSourceSpec extends Specification {

    static final Logger log = Logging.getLogger(MongoDBFeatureSourceSpec.class.getPackage().getName())

    @Shared FeatureType locationFeatureType
    @Shared FeatureType poleFeatureType
    @Shared DB database
    @Shared BasicDBObject locationJSON
    @Shared BasicDBObject designJSON
    @Shared MongoDBDataAccess MongoDBDataAccess
    @Shared MongoDBFeatureSource locationFeatureSource
    @Shared MongoDBFeatureSource poleFeatureSource
    @Shared BasicDBList jsonMapping
    @Shared String namespace = "http://spida/db"

    void setupSpec() {
        locationJSON = JSON.parse(getClass().getResourceAsStream('/location.json').text)
        designJSON = JSON.parse(getClass().getResourceAsStream('/design.json').text)
        String host = System.getProperty("mongoHost")
        String port = System.getProperty("mongoPort")
        String databaseName = System.getProperty("mongoDatabase")
        def serverAddress = new ServerAddress(host, Integer.valueOf(port))
        MongoClient mongoClient = new MongoClient(serverAddress)
        jsonMapping = JSON.parse(getClass().getResourceAsStream('/mapping.json').text)
        mongoDBDataAccess = new MongoDBDataAccess(namespace, host, port, databaseName, null, null, jsonMapping)
        database = mongoClient.getDB(databaseName)
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
        database.getCollection("locations").insert(locationJSON)
        database.getCollection("designs").remove(new BasicDBObject("id", designJSON.get("id")))
        database.getCollection("designs").insert(designJSON)

        locationFeatureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "location"))
        poleFeatureType = mongoDBDataAccess.getSchema(new NameImpl(namespace, "pole"))
        locationFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, locationFeatureType, jsonMapping.find { it.typeName == "location" })
        poleFeatureSource = new MongoDBFeatureSource(mongoDBDataAccess, database, poleFeatureType, jsonMapping.find { it.typeName == "pole" })
    }

    void cleanupSpec () {
        database.getCollection("locations").remove(new BasicDBObject("id", locationJSON.get("id")))
    }

    void "get getFeatures for location no filter or query"() {
        when:
            FeatureCollection featureCollection = locationFeatureSource.getFeatures()
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == database.getCollection("locations").count
    }

    void "get getFeatures for location with filter"() {
        when:
            FeatureCollection featureCollection = locationFeatureSource.getFeatures(CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'"))
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == 1
    }

    void "get getFeatures for location with query"() {
        when:
            FeatureCollection featureCollection = locationFeatureSource.getFeatures(new Query("location", CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'")))
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == 1
    }

    void "get getBounds for location with query"() {
        when:
            Query query = new Query("location", CQL.toFilter("id='55fac7fde4b0e7f2e3be342c'"))
            ReferencedEnvelope referencedEnvelope = locationFeatureSource.getBounds(query)
        then:
            referencedEnvelope instanceof ReferencedEnvelope
            referencedEnvelope.getDimension() == 2
            referencedEnvelope.getMinimum(0) == -118.3824234008789
            referencedEnvelope.getMaximum(0) == -118.3824234008789
            referencedEnvelope.getMinimum(1) == 33.80541229248047
            referencedEnvelope.getMaximum(1) == 33.80541229248047
    }

    void "test getFeatures for pole with query"() {
        when:
            Query query = new Query("pole", CQL.toFilter("locationId IN=('55fac7fde4b0e7f2e3be342c')"))
            FeatureCollection featureCollection = poleFeatureSource.getFeatures(query)
        then:
            featureCollection instanceof MongoDBFeatureCollection
            featureCollection.size() == 1
    }

    void "get getBounds for pole with query"() {
        when:
            Query query = new Query("pole", CQL.toFilter("id='56e9b7137d84511d8dd0f13c'"))
            ReferencedEnvelope referencedEnvelope = poleFeatureSource.getBounds(query)
        then:
            referencedEnvelope instanceof ReferencedEnvelope
            referencedEnvelope.getDimension() == 2
            referencedEnvelope.getMinimum(0) == 0.0
            referencedEnvelope.getMaximum(0) == -1.0
            referencedEnvelope.getMinimum(1) == 0.0
            referencedEnvelope.getMaximum(1) == -1.0
    }
}
