#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <stdio.h>
#include "sock.h"

/*****************************************
* Function: sock_daemon_connect
*****************************************/
int sock_daemon_connect(
	int port)
{
	struct addrinfo *res, *t;
	struct addrinfo hints;
    memset( &hints, 0, sizeof(struct addrinfo) );
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

	char *service;
	int n;
	int sockfd = -1, connfd;

	if (asprintf(&service, "%d", port) < 0) {
		fprintf(stderr, "asprintf failed\n");
		return -1;
	}

	n = getaddrinfo(NULL, service, &hints, &res);
	if (n < 0) {
		fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
		free(service);
		return -1;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			n = 1;

			setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

			if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}

	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		fprintf(stderr, "couldn't listen to port %d\n", port);
		return -1;
	}

	listen(sockfd, 1);
	connfd = accept(sockfd, NULL, 0);
	close(sockfd);
	if (connfd < 0) {
		fprintf(stderr, "accept() failed\n");
		return -1;
	}

	return connfd;
}

/*****************************************
* Function: sock_client_connect
*****************************************/
int sock_client_connect(
	const char *server_name,
	int port)
{
	struct addrinfo *res, *t;
	struct addrinfo hints;

	memset( &hints, 0, sizeof(struct addrinfo) );
	hints.ai_flags = AI_PASSIVE;
	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	char *service;
	int n;
	int sockfd = -1;


	if (asprintf(&service, "%d", port) < 0) {
		fprintf(stderr, "asprintf failed\n");
		return -1;
	}

	n = getaddrinfo(server_name, service, &hints, &res);
	if (n < 0) {
		fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), server_name, port);
		free(service);
		return -1;
	}

	for (t = res; t; t = t->ai_next) {
		sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
		if (sockfd >= 0) {
			if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
				break;
			close(sockfd);
			sockfd = -1;
		}
	}
	freeaddrinfo(res);
	free(service);

	if (sockfd < 0) {
		// fprintf(stderr, "couldn't connect to %s:%d\n", server_name, port);
		// fprintf(stderr, "couldn't connect to %s:%d. Retry in 3 seconds.\n", server_name, port);
		return -1;
	}

	return sockfd;
}

/*****************************************
* Function: sock_recv
*****************************************/
int sock_recv(
	int sock_fd,
	size_t size,
	void *buf)
{
	int rc;

retry_after_signal:
	rc = recv(sock_fd, buf, size, MSG_WAITALL);
	if (rc != (int)size) {
		fprintf(stderr, "recv failed: %s, rc=%d\n", strerror(errno), rc);

		if ((errno == EINTR) && (rc != 0))
			goto retry_after_signal;    /* Interrupted system call */
		if (rc)
			return rc;
		else
			return -1;
	}

	return 0;
}

/*****************************************
* Function: sock_send
*****************************************/
int sock_send(
	int sock_fd,
	size_t size,
	const void *buf)
{
	int rc;


retry_after_signal:
	rc = send(sock_fd, buf, size, 0);

	if (rc != (int)size) {
		fprintf(stderr, "send failed: %s, rc=%d\n", strerror(errno), rc);

		if ((errno == EINTR) && (rc != 0))
			goto retry_after_signal;    /* Interrupted system call */
		if (rc)
			return rc;
		else
			return -1;
	}

	return 0;
}

/*****************************************
* Function: sock_sync_data
*****************************************/
int sock_sync_data(
	int sock_fd,
	int is_daemon,
	size_t size,
	const void *out_buf,
	void *in_buf)
{
	int rc;


	if (is_daemon) {
		rc = sock_send(sock_fd, size, out_buf);
		if (rc)
			return rc;

		rc = sock_recv(sock_fd, size, in_buf);
		if (rc)
			return rc;
	} else {
		rc = sock_recv(sock_fd, size, in_buf);
		if (rc)
			return rc;

		rc = sock_send(sock_fd, size, out_buf);
		if (rc)
			return rc;
	}

	return 0;
}

/*****************************************
* Function: sock_sync_ready
*****************************************/
int sock_sync_ready(
	int sock_fd,
	int is_daemon)
{
	char cm_buf = 'a';


	return sock_sync_data(sock_fd, is_daemon, sizeof(cm_buf), &cm_buf, &cm_buf);
}

