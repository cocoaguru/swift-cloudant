//
//  InterceptableSessionTests.swift
//  SwiftCloudant
//
//  Created by Rhys Short on 19/08/2016.
//
//

import Foundation
import XCTest
@testable import SwiftCloudant


class InterceptableSessionTests : XCTestCase {
    
    
    lazy var sessionConfig = {() -> URLSessionConfiguration in
        let config = URLSessionConfiguration.default
        config.protocolClasses = [BackOffHTTPURLProtocol.self]
        return config
    
    }()
    
    func testInterceptableSessionBacksOff() throws {
        let session = InterceptableSession(delegate: nil, configuration: InterceptableSessionConfiguration(shouldBackOff:true))
        session.session = URLSession(configuration: sessionConfig, delegate: session, delegateQueue: nil)
        
        guard let url = URL(string:"http://example.com") else {
            XCTFail("Failed to create url with http://example.com")
            return
        }
        
        let request = URLRequest(url: url)
        
        
        let delegate = BackOffRequestDelegate(expectation: self.expectation(description:"429 back off request"))
        let task = session.dataTask(request: request, delegate: delegate)
        task.resume() // start the task processing.
        
        self.waitForExpectations(timeout: 10.0)
        
        XCTAssertEqual(2, remaingBackOffRetries(for: task))
        XCTAssertEqual(200, delegate.response?.statusCode)
        XCTAssertNil(delegate.error)
        XCTAssertNotNil(delegate.data)
        XCTAssertEqual(9, remaingTotalRetries(for: task))
    }
    
    
    func testInterceptableSessionBackOffMax() throws {
        let config = sessionConfig
        config.protocolClasses = [AlwaysBackOffHTTPURLProtocol.self]
        
        let session = InterceptableSession(delegate: nil, configuration: InterceptableSessionConfiguration(shouldBackOff:true))
        session.session = URLSession(configuration: config, delegate: session, delegateQueue: nil)
        
        guard let url = URL(string:"http://example.com") else {
            XCTFail("Failed to create url with http://example.com")
            return
        }
        
        let request = URLRequest(url: url)
        
        
        let delegate = BackOffRequestDelegate(expectation: self.expectation(description:"429 back off request"))
        let task = session.dataTask(request: request, delegate: delegate)
        task.resume() // start the task processing.
        
        self.waitForExpectations(timeout: 10.0)
        
        XCTAssertEqual(0, remaingBackOffRetries(for: task))
        XCTAssertEqual(429, delegate.response?.statusCode)
        XCTAssertNil(delegate.error)
        XCTAssertNotNil(delegate.data)
        XCTAssertEqual(7, remaingTotalRetries(for: task))
    }
    
    func testBackSetHigherThanAllowedRetires() throws {
        let config = sessionConfig
        config.protocolClasses = [AlwaysBackOffHTTPURLProtocol.self]
        
        let session = InterceptableSession(delegate: nil, configuration: InterceptableSessionConfiguration(totalRetries: 3, shouldBackOff:true, backOffRetires: 4))
        session.session = URLSession(configuration: config, delegate: session, delegateQueue: nil)
        
        guard let url = URL(string:"http://example.com") else {
            XCTFail("Failed to create url with http://example.com")
            return
        }
        
        let request = URLRequest(url: url)
        
        
        let delegate = BackOffRequestDelegate(expectation: self.expectation(description:"429 back off request"))
        let task = session.dataTask(request: request, delegate: delegate)
        task.resume() // start the task processing.
        
        self.waitForExpectations(timeout: 20.0)
        
        XCTAssertEqual(1, remaingBackOffRetries(for: task))
        XCTAssertEqual(429, delegate.response?.statusCode)
        XCTAssertNil(delegate.error)
        XCTAssertNotNil(delegate.data)
        XCTAssertEqual(0, remaingTotalRetries(for: task))
    }
    
    func testInterceptableSessionNoBackOff() throws {
        let config = sessionConfig
        config.protocolClasses = [AlwaysBackOffHTTPURLProtocol.self]
        let session = InterceptableSession(delegate: nil, configuration: InterceptableSessionConfiguration(shouldBackOff: false))
        session.session = URLSession(configuration: config, delegate: session, delegateQueue: nil)
        
        guard let url = URL(string:"http://example.com") else {
            XCTFail("Failed to create url with http://example.com")
            return
        }
        
        let request = URLRequest(url: url)
        
        
        let delegate = BackOffRequestDelegate(expectation: self.expectation(description:"429 back off request"))
        let task = session.dataTask(request: request, delegate: delegate)
        task.resume() // start the task processing.
        
        self.waitForExpectations(timeout: 10.0)
        
        XCTAssertEqual(3, remaingBackOffRetries(for: task))
        XCTAssertEqual(429, delegate.response?.statusCode)
        XCTAssertNil(delegate.error)
        XCTAssertNotNil(delegate.data)
        XCTAssertEqual(10, remaingTotalRetries(for: task))
    }
    
    func remaingBackOffRetries(for task: SwiftCloudant.URLSessionTask) -> Int {
        return value(of: "remainingBackOffRetires", for: task)
    }
    
    func remaingTotalRetries(for task: SwiftCloudant.URLSessionTask) -> Int {
        return value(of: "remainingRetries", for: task)
    }
    
    func value(of key:String, for task: SwiftCloudant.URLSessionTask) -> Int {
        let mirror = Mirror(reflecting: task);
        let values = mirror.children.filter { (innerKey, value) in
                return innerKey == key
            }.first
        
        if let value = values?.value as? UInt {
            return Int(value)
        }
        
        return -1
    }
    
    
}

// We need reference type semantics here.
class BackOffRequestDelegate: InterceptableSessionDelegate {
    
    private let expectation:XCTestExpectation
    var response: HTTPURLResponse?
    var data: Data = Data()
    var error: Error?
    
    init(expectation: XCTestExpectation){
        self.expectation = expectation
    }
    
    
    func received(response:HTTPURLResponse) {
        self.response = response
    }
    
    func received(data: Data){
        self.data.append(data)
    }
    
    func completed(error: Swift.Error?){
        self.error = error
        self.expectation.fulfill()
    }
}


class BackOffHTTPURLProtocol: CookieSessionHTTPURLProtocol {
    
    static var shouldBackOff = true
    
    override class func canInit(with request: URLRequest) -> Bool {
        return request.url!.host! == "example.com"
    }
    
    override func startLoading() {
        if BackOffHTTPURLProtocol.shouldBackOff {
            sendResponse(statusCode: 429, json: [:])
            BackOffHTTPURLProtocol.shouldBackOff = false
        } else {
            BackOffHTTPURLProtocol.shouldBackOff = true
            sendResponse(statusCode: 200, json: [:])
            
        }
    }
}

class AlwaysBackOffHTTPURLProtocol: CookieSessionHTTPURLProtocol {
    
    
    override class func canInit(with request: URLRequest) -> Bool {
        return request.url!.host! == "example.com"
    }
    
    override func startLoading() {
        sendResponse(statusCode: 429, json: [:])
    }
}


