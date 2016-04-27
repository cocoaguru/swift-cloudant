//
//  RequestBuilder.swift
//  ObjectiveCloudant
//
//  Created by Rhys Short on 03/03/2016.
//  Copyright (c) 2016 IBM Corp.
//
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
//


import Foundation


// TODO rename to something that makes a little more swift sense.
/**
  Designates an operation which provides data to perform a HTTP Request.
 */
protocol HTTPRequestOperation {
    
    /**
     Provides the `InterceptableSession` to use when making HTTP requests.
     */
    var session:InterceptableSession { get }
    /**
     The root of url, e.g. `example.cloudant.com`
     */
    var rootURL:NSURL { get}
    /**
     The path of the url e.g. `/exampledb/document1/`
    */
    var httpPath:String { get }
    /**
     The method to use for the HTTP request e.g. `GET`
     */
    var httpMethod : String { get }
    /**
        The query items to use for the request
     */
    var queryItems:[NSURLQueryItem] { get }
    
    /**
        The body of the HTTP request or `nil` if there is no data for the request.
     */
    var httpRequestBody:NSData? { get }
    
    /**
     A function that is called when the operation is completed.
     */
    func completeOperation()
    /**
      A function to process the response from a HTTP request.
     
     - parameter data: The data returned from the HTTP request or nil if there was an error.
     - parameter statusCode: The statusCode of the HTTP response.
     - parameter error: A type representing an error if one occurred or `nil`
     */
    func processResponse(data:NSData?, statusCode:Int, error:ErrorProtocol?);
    
    var isCancelled: Bool { get }
    
}

/**
    A class which builds `NSURLRequest` objects from `HTTPRequestOperation` objects.
 */
class OperationRequestBuilder {
    
    /**
     The operation this builder will turn into a HTTP object.
    */
    let operation:HTTPRequestOperation
    
    /**
     Creates an OperationRequestBuilder instance.
     
     - parameter operation: the operation that the request will be built from.
    */
    init(operation:HTTPRequestOperation){
        self.operation = operation
    }
    
    /**
        Builds the NSURLRequest from the operation in the property `operation`
     */
    func buildRequest() -> NSURLRequest {
        guard let components = NSURLComponents(url: operation.rootURL, resolvingAgainstBaseURL: false)
        else {
            //crash for now
            abort()
        }
        components.path = operation.httpPath
        var queryItems : [NSURLQueryItem] = []
        
        if let _ = components.queryItems {
            queryItems.append(contentsOf:components.queryItems!)
        }
        
        queryItems.append(contentsOf:operation.queryItems)
        components.queryItems = queryItems
        
        guard let url = components.url
        else {
            // crash for now
            abort()
        }
        let request = NSMutableURLRequest(url: url)
        request.cachePolicy = .useProtocolCachePolicy
        request.timeoutInterval = 10.0
        request.httpMethod = operation.httpMethod
        
        if let body = operation.httpRequestBody {
            request.httpBody = body
        }
        
        
        return request

    }
    
}



































