












/**
 * HOF Takes a generic collection of Kinesis stream events, parses them and processes them with the included
 * function and target medium (ie: the persistence mechanism used by the passed in event
 * @param streamEvents
 * @param eventProcessorFunction
 * @param target
 * @returns {Promise<any>}
 */
async function streamEventPromisifier( streamEvents, eventProcessorFunction, target ) {
  return new Promise(( resolve, reject ) => {
    let parsedEvents = streamEvents.map(event => {
      return extractKinesisEvent( event );
    });
    let processEventsPromiseArray = parsedEvents.map(
      function( event ) { return eventProcessorFunction( event, target )}
    );
    Promise.all( processEventsPromiseArray )
      .then( () => {
        logger.info( 'successfully processed all events' );
        resolve()
      })
      .catch( err => {
        logger.error( 'error processing events ', err );
        reject( err )
      })
  })
} // end streamEventPromisifier










async function queueEventPromisifier( streamEvents, eventProcessorFunction, target ) {
  return new Promise(( resolve, reject ) => {
    let parsedEvents = streamEvents.map( event => {
      return extractQueueEvent( event );
    });
    let processEventsPromiseArray = parsedEvents.map(
      function( event ) { return eventProcessorFunction( event, target )}
    );
    Promise.all( processEventsPromiseArray )
      .then( () => {
        logger.info( 'successfully processed all events' );
        resolve()
      })
      .catch( err => {
        logger.error( 'error processing events ', err );
        reject( err )
      })
  })
} // end queueEventPromisifier









/**
 * parses a kinesis stream event
 * @param incomingKinesisEvent
 * @returns {any}
 */
function extractKinesisEvent( incomingKinesisEvent ) {
  let incomingKinesisPayload = JSON.parse( new Buffer( incomingKinesisEvent.kinesis.data, BASE64 ));
  return incomingKinesisPayload;
} // end extractKinesisEvent












/**
 * parses a SQS queue event
 * @param incomingQueueEvent
 * @returns {any}
 */
function extractQueueEvent( incomingQueueEvent ) {
  let incomingQueuePayload = JSON.parse( incomingQueueEvent.body );
  incomingQueuePayload.messageId = incomingQueueEvent.messageId;
  incomingQueuePayload.sentTime = incomingQueueEvent.attributes.SentTimestamp;
  logger.info('extractQueueEvent resp : ', incomingQueuePayload);
  return incomingQueuePayload
}





