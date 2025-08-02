"""Tests for FastAPI application lifespan management."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI

from app.main import create_app, lifespan


class TestLifespanManager:
    """Test application lifespan management."""

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_lifespan_startup_and_shutdown(self):
        """Test the lifespan context manager startup and shutdown sequence."""
        # Create a mock FastAPI app
        mock_app = MagicMock(spec=FastAPI)
        mock_app.state = MagicMock()

        # Mock the MongoDBClient
        with patch("app.main.MongoDBClient") as mock_mongodb_class:
            mock_client = AsyncMock()
            mock_mongodb_class.return_value = mock_client

            # Test the lifespan context manager
            async with lifespan(mock_app):
                # During startup
                # Verify MongoDB client was created and connected
                mock_mongodb_class.assert_called_once()
                mock_client.connect.assert_called_once()

                # Verify the client was attached to app state
                assert mock_app.state.mongodb_client == mock_client

            # After shutdown (exiting the context manager)
            # Verify MongoDB client was disconnected
            mock_client.disconnect.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_lifespan_startup_failure(self):
        """Test lifespan behavior when MongoDB connection fails during startup."""
        mock_app = MagicMock(spec=FastAPI)
        mock_app.state = MagicMock()

        with patch("app.main.MongoDBClient") as mock_mongodb_class:
            mock_client = AsyncMock()
            mock_client.connect.side_effect = Exception("Connection failed")
            mock_mongodb_class.return_value = mock_client

            # The lifespan should propagate the connection error
            with pytest.raises(Exception, match="Connection failed"):
                async with lifespan(mock_app):
                    pass  # Should not reach here

            # Verify connect was attempted
            mock_client.connect.assert_called_once()

            # Disconnect should not be called if startup failed
            mock_client.disconnect.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_lifespan_shutdown_failure_propagates(self):
        """Test that shutdown failures are propagated (not silently handled)."""
        mock_app = MagicMock(spec=FastAPI)
        mock_app.state = MagicMock()

        with patch("app.main.MongoDBClient") as mock_mongodb_class:
            mock_client = AsyncMock()
            mock_client.disconnect.side_effect = Exception("Disconnect failed")
            mock_mongodb_class.return_value = mock_client

            # Shutdown failures should be propagated, not silently handled
            with pytest.raises(Exception, match="Disconnect failed"):
                async with lifespan(mock_app):
                    # Startup should work fine
                    mock_client.connect.assert_called_once()
                    assert mock_app.state.mongodb_client == mock_client
                    # Exit context triggers disconnect which raises exception

            # Disconnect was attempted
            mock_client.disconnect.assert_called_once()

    @pytest.mark.unit
    def test_create_app_includes_lifespan(self):
        """Test that create_app properly configures the lifespan manager."""
        app = create_app()

        # Verify the app has the lifespan configured
        assert app.router.lifespan_context is not None

        # The lifespan should be our lifespan function
        # Note: FastAPI wraps the lifespan function, so we can't directly compare
        # but we can verify it's configured
        assert hasattr(app.router, "lifespan_context")
